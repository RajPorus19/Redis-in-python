import socket
import time
import threading

from .resp import (
    _encode_simple_string,
    _encode_bulk_string,
    _encode_array,
    _encode_integer,
    _encode_null_array,
)


# In-memory key-value store for SET/GET
_kv_store: dict[bytes, bytes] = {}
_kv_expiry: dict[bytes, float] = {}
_kv_lock = threading.Lock()

# In-memory list store for RPUSH/LPUSH and other list commands
_list_store: dict[bytes, list[bytes]] = {}
_list_lock = threading.Lock()

# BLPOP waiters per list key (FIFO): key -> list of (connection, event)
_blpop_waiters: dict[bytes, list[tuple[socket.socket, threading.Event]]] = {}
_blpop_lock = threading.Lock()

# dict for streams
_stream_store: dict[bytes, list[dict[bytes, bytes]]] = {}


def _handle_ping(connection: socket.socket) -> None:
    connection.sendall(_encode_simple_string(b"PONG"))


def _handle_echo(connection: socket.socket, array: list) -> None:
    arg = array[1] if len(array) >= 2 else None
    if isinstance(arg, (bytes, bytearray)):
        connection.sendall(_encode_bulk_string(bytes(arg)))
    else:
        connection.sendall(_encode_bulk_string(None))


def _parse_set_expiry_ms(array: list, start_index: int = 3) -> int | None:
    """Parse PX/EX options for SET and return expiry in milliseconds."""
    expiry_ms: int | None = None
    i = start_index
    while i < len(array):
        opt_raw = array[i]
        if not isinstance(opt_raw, (bytes, bytearray)):
            i += 1
            continue
        opt = bytes(opt_raw).upper()

        # PX <ms>
        if opt == b"PX" and i + 1 < len(array):
            next_raw = array[i + 1]
            if isinstance(next_raw, (bytes, bytearray)):
                try:
                    expiry_ms = int(bytes(next_raw))
                except ValueError:
                    pass
            i += 2
            continue

        # EX <seconds>
        if opt == b"EX" and i + 1 < len(array):
            next_raw = array[i + 1]
            if isinstance(next_raw, (bytes, bytearray)):
                try:
                    expiry_ms = int(bytes(next_raw)) * 1000
                except ValueError:
                    pass
            i += 2
            continue

        # Unknown/unsupported option, skip
        i += 1

    return expiry_ms


def _handle_set(connection: socket.socket, array: list) -> None:
    if len(array) < 3:
        # Minimal error handling: reply OK to stay permissive
        connection.sendall(_encode_simple_string(b"OK"))
        return
    key_raw = array[1]
    val_raw = array[2]
    if isinstance(key_raw, (bytes, bytearray)) and isinstance(
        val_raw, (bytes, bytearray)
    ):
        key = bytes(key_raw)
        value = bytes(val_raw)
        # Parse optional PX/EX arguments into milliseconds
        expiry_ms: int | None = _parse_set_expiry_ms(array)

        with _kv_lock:
            _kv_store[key] = value
            if expiry_ms is not None and expiry_ms >= 0:
                _kv_expiry[key] = time.time() + (expiry_ms / 1000.0)
            else:
                # Clear any previous expiry if no expiry provided in this SET
                _kv_expiry.pop(key, None)
        connection.sendall(_encode_simple_string(b"OK"))
    else:
        connection.sendall(_encode_simple_string(b"OK"))


def _handle_get(connection: socket.socket, array: list) -> None:
    if len(array) < 2:
        connection.sendall(_encode_bulk_string(None))
        return
    key_raw = array[1]
    if isinstance(key_raw, (bytes, bytearray)):
        key = bytes(key_raw)
        with _kv_lock:
            # Check expiry and purge if necessary
            deadline = _kv_expiry.get(key)
            if deadline is not None and time.time() >= deadline:
                _kv_store.pop(key, None)
                _kv_expiry.pop(key, None)
                value = None
            else:
                value = _kv_store.get(key)
        connection.sendall(_encode_bulk_string(value if value is not None else None))
    else:
        connection.sendall(_encode_bulk_string(None))


def _handle_push(connection: socket.socket, array: list, command: str) -> None:
    """Handle RPUSH/LPUSH for creating/appending with one or more elements.

    Supports: RPUSH <key> <element1> [element2 ...].
    Creates the list if it does not exist and appends all provided elements.
    Returns the resulting length as a RESP integer.
    """
    if len(array) < 3:
        # Minimal handling: reply with 0 as integer
        connection.sendall(_encode_integer(0))
        return

    key_raw = array[1]
    # Collect all valid byte-like elements from position 2 onwards
    values_raw = array[2:]
    if not isinstance(key_raw, (bytes, bytearray)):
        connection.sendall(_encode_integer(0))
        return

    # Filter to byte-like values and convert to bytes
    elements: list[bytes] = []
    for v in values_raw:
        if isinstance(v, (bytes, bytearray)):
            elements.append(bytes(v))

    if not elements:
        connection.sendall(_encode_integer(0))
        return

    key = bytes(key_raw)

    with _list_lock:
        lst = _list_store.get(key)
        if lst is None:
            lst = []
            _list_store[key] = lst
        if command == "RPUSH":
            lst.extend(elements)
        elif command == "LPUSH":
            # Prepend each element in the order provided by the client
            for e in elements:
                lst.insert(0, e)
        else:
            raise ValueError(f"Unknown command: {command}")
        new_len = len(lst)

    # Send push result first (should reflect length after push)
    connection.sendall(_encode_integer(new_len))

    # After responding, try waking BLPOP waiters (one per pushed element)
    for _ in range(len(elements)):
        if not _try_wake_blpop_waiter(key):
            break


def _handle_rpush(connection: socket.socket, array: list) -> None:
    _handle_push(connection, array, "RPUSH")


def _handle_lpush(connection: socket.socket, array: list) -> None:
    _handle_push(connection, array, "LPUSH")


def _handle_lrange(connection: socket.socket, array: list) -> None:
    if len(array) < 4:
        connection.sendall(_encode_array([]))
        return
    list_key_raw = array[1]
    start_raw = array[2]
    end_raw = array[3]
    if not isinstance(list_key_raw, (bytes, bytearray)):
        connection.sendall(_encode_array([]))
        return
    # Convert indexes; assume valid integers per stage requirements
    start = int(start_raw)
    end = int(end_raw)
    list_key = bytes(list_key_raw)
    with _list_lock:
        lst = _list_store.get(list_key)
        if lst is None:
            connection.sendall(_encode_array([]))
            return
        n = len(lst)
        # Translate negative indexes from the end of list
        if start < 0:
            start = n + start
        if end < 0:
            end = n + end
        # Clamp to valid bounds
        if start < 0:
            start = 0
        if end >= n:
            end = n - 1
        if start > end or n == 0:
            connection.sendall(_encode_array([]))
            return
        range_list = lst[start : end + 1]
        connection.sendall(_encode_array(range_list))


def _handle_llen(connection: socket.socket, array: list) -> None:
    if len(array) < 2:
        connection.sendall(_encode_integer(0))
        return
    list_key_raw = array[1]
    if not isinstance(list_key_raw, (bytes, bytearray)):
        connection.sendall(_encode_integer(0))
        return
    list_key = bytes(list_key_raw)
    with _list_lock:
        lst = _list_store.get(list_key)
        if lst is None:
            connection.sendall(_encode_integer(0))
            return
        length = len(lst)
        connection.sendall(_encode_integer(length))


def _handle_lpop(connection: socket.socket, array: list) -> None:
    if len(array) < 2:
        connection.sendall(_encode_bulk_string(None))
        return
    list_key_raw = array[1]
    if not isinstance(list_key_raw, (bytes, bytearray)):
        connection.sendall(_encode_bulk_string(None))
        return
    list_key = bytes(list_key_raw)
    count: int | None = None
    if len(array) > 2:
        count_raw = array[2]
        if isinstance(count_raw, (bytes, bytearray)):
            try:
                count = int(bytes(count_raw))
            except ValueError:
                count = None
    with _list_lock:
        lst = _list_store.get(list_key)
        if lst is None or len(lst) == 0:
            if count is not None:
                connection.sendall(_encode_array([]))
            else:
                connection.sendall(_encode_bulk_string(None))
            return
        if count is None:
            # Pop a single element from head
            element = lst.pop(0)
            connection.sendall(_encode_bulk_string(element))
            return
        # Pop multiple from head, return array
        if count <= 0:
            connection.sendall(_encode_array([]))
            return
        num_to_pop = min(count, len(lst))
        popped: list[bytes] = []
        for _ in range(num_to_pop):
            popped.append(lst.pop(0))
        connection.sendall(_encode_array(popped))


def _handle_blpop(connection: socket.socket, array: list) -> None:
    # Expect: BLPOP <key> <timeout>
    if len(array) < 3:
        connection.sendall(_encode_null_array())
        return
    key_raw = array[1]
    timeout_raw = array[2]
    if not isinstance(key_raw, (bytes, bytearray)):
        connection.sendall(_encode_null_array())
        return
    key = bytes(key_raw)
    # Timeout is specified in seconds and may be fractional (e.g. "0.1").
    # Parse it as a float; non-numeric values fall back to 0, which we treat
    # as "block indefinitely".
    if isinstance(timeout_raw, (bytes, bytearray)):
        try:
            timeout = float(bytes(timeout_raw))
        except ValueError:
            timeout = 0.0
    else:
        try:
            timeout = float(timeout_raw)
        except (TypeError, ValueError):
            timeout = 0.0

    # Fast-path: if element exists, pop immediately
    with _list_lock:
        lst = _list_store.get(key)
        if lst:
            value = lst.pop(0)
            connection.sendall(_encode_array([key, value]))
            return

    # Otherwise, register as waiter and block indefinitely (timeout==0)
    event = threading.Event()
    with _blpop_lock:
        waiters = _blpop_waiters.get(key)
        if waiters is None:
            waiters = []
            _blpop_waiters[key] = waiters
        waiters.append((connection, event))

    # Wait until a pusher wakes us (response will be sent by the waker)
    if timeout == 0:
        event.wait()
        return
    # For non-zero timeouts, wait up to the specified number of seconds.
    signaled = event.wait(timeout=max(0.0, timeout))
    if not signaled:
        # Timed out: remove ourselves if still queued and reply with null array
        with _blpop_lock:
            waiters = _blpop_waiters.get(key)
            if waiters is not None:
                try:
                    waiters.remove((connection, event))
                except ValueError:
                    pass
        connection.sendall(_encode_null_array())


def _try_wake_blpop_waiter(key: bytes) -> bool:
    # Attempt to wake the earliest waiter for this key by popping one value
    # from the head and sending [key, value]. Returns True if someone was woken.
    # Step 1: get waiter if any
    with _blpop_lock:
        waiters = _blpop_waiters.get(key)
        if not waiters:
            return False
        waiter_conn, waiter_event = waiters.pop(0)
    # Step 2: pop one value from list head
    with _list_lock:
        lst = _list_store.get(key)
        if not lst:
            # No value available; push waiter back and abort
            with _blpop_lock:
                _blpop_waiters.setdefault(key, []).insert(
                    0, (waiter_conn, waiter_event)
                )
            return False
        value = lst.pop(0)
    # Step 3: respond to waiter and signal event
    try:
        waiter_conn.sendall(_encode_array([key, value]))
    finally:
        waiter_event.set()
    return True

def _handle_type(connection: socket.socket, array: list) -> None:
    if len(array) < 2:
        connection.sendall(_encode_simple_string(b"none"))
        return

    key_raw = array[1]
    if not isinstance(key_raw, (bytes, bytearray)):
        connection.sendall(_encode_simple_string(b"none"))
        return

    key = bytes(key_raw)

    # TODO : this needs to be refactored as it's called in the get and here, and this passive check is repeated
    with _kv_lock:
        deadline = _kv_expiry.get(key)
        if deadline is not None and time.time() >= deadline:
            # Expired – purge and treat as non-existent.
            _kv_store.pop(key, None)
            _kv_expiry.pop(key, None)
            value_exists = False
        else:
            value_exists = key in _kv_store

    if value_exists:
        connection.sendall(_encode_simple_string(b"string"))
        return

    # No value found for this key.
    connection.sendall(_encode_simple_string(b"none"))


def _handle_xadd(connection: socket.socket, array: list) -> None:
    if len(array) < 3:
        connection.sendall(_encode_simple_string(b"none"))
        return
    stream_key_raw = array[1]
    element_id_raw = array[2]
    if not isinstance(stream_key_raw, (bytes, bytearray)):
        connection.sendall(_encode_simple_string(b"none"))
        return
    if not isinstance(element_id_raw, (bytes, bytearray)):
        connection.sendall(_encode_simple_string(b"none"))
        return
    stream_key = bytes(stream_key_raw)
    element_id = bytes(element_id_raw)
    _stream_store[stream_key].append({element_id: {}})
    connection.sendall(_encode_simple_string(element_id))

def _dispatch_array_command(connection: socket.socket, array: list) -> None:
    """Handle RESP Array-based commands like PING and ECHO."""
    if not array:
        return
    cmd_raw = array[0]
    if not isinstance(cmd_raw, (bytes, bytearray)):
        return
    cmd = bytes(cmd_raw).upper()

    if cmd == b"PING":
        _handle_ping(connection)
        return

    if cmd == b"ECHO" and len(array) >= 2:
        _handle_echo(connection, array)
        return

    if cmd == b"SET" and len(array) >= 3:
        _handle_set(connection, array)
        return

    if cmd == b"GET" and len(array) >= 2:
        _handle_get(connection, array)
        return

    if cmd == b"RPUSH" and len(array) >= 3:
        _handle_rpush(connection, array)
        return

    if cmd == b"LPUSH" and len(array) >= 3:
        _handle_lpush(connection, array)
        return

    if cmd == b"LRANGE" and len(array) >= 4:
        _handle_lrange(connection, array)
        return

    if cmd == b"LLEN" and len(array) >= 2:
        _handle_llen(connection, array)
        return

    if cmd == b"LPOP" and len(array) >= 2:
        _handle_lpop(connection, array)
        return

    if cmd == b"BLPOP" and len(array) >= 3:
        _handle_blpop(connection, array)
        return

    if cmd == b"TYPE" and len(array) >= 2:
        _handle_type(connection, array)
        return

    if cmd == b"XADD" and len(array) >= 3:
        _handle_xadd(connection, array)
        return


def _process_frame(connection: socket.socket, frame) -> None:
    """Process a single RESP frame and respond if it's a recognized command."""
    if isinstance(frame, list):
        _dispatch_array_command(connection, frame)
