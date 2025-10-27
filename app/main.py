import socket
import time
import threading

# In-memory key-value store for SET/GET
_kv_store: dict[bytes, bytes] = {}
_kv_expiry: dict[bytes, float] = {}
_kv_lock = threading.Lock()

# In-memory list store for RPUSH
_list_store: dict[bytes, list[bytes]] = {}
_list_lock = threading.Lock()

# BLPOP waiters per list key (FIFO): key -> list of (connection, event)
_blpop_waiters: dict[bytes, list[tuple[socket.socket, threading.Event]]] = {}
_blpop_lock = threading.Lock()


def _find_crlf(data: bytearray, start: int) -> int:
    idx = data.find(b"\r\n", start)
    return idx


def _parse_int_line(data: bytearray, start: int):
    end = _find_crlf(data, start)
    if end == -1:
        return None
    try:
        value = int(data[start:end])
    except ValueError:
        return None
    return value, end + 2


def _parse_simple_string(data: bytearray, start: int):
    # +<string>\r\n
    end = _find_crlf(data, start)
    if end == -1:
        return None
    return bytes(data[start:end]), end + 2


def _parse_bulk_string(data: bytearray, start: int):
    # $<len>\r\n<bytes>\r\n
    res = _parse_int_line(data, start)
    if res is None:
        return None
    length, idx = res
    if length == -1:
        return None, idx  # Null bulk string
    end = idx + length
    if len(data) < end + 2:
        return None
    if data[end : end + 2] != b"\r\n":
        return None
    return bytes(data[idx:end]), end + 2


def _parse_integer(data: bytearray, start: int):
    return _parse_int_line(data, start)


def _parse_frame_at(data: bytearray, start: int):
    if start >= len(data):
        return None
    prefix = data[start : start + 1]
    if not prefix:
        return None
    p = prefix[0]
    idx = start + 1
    if p == ord(b"+"):
        res = _parse_simple_string(data, idx)
        if res is None:
            return None
        value, next_idx = res
        return value, next_idx
    if p == ord(b"-"):
        # Parse error string as bytes
        res = _parse_simple_string(data, idx)
        if res is None:
            return None
        value, next_idx = res
        return value, next_idx
    if p == ord(b":"):
        res = _parse_integer(data, idx)
        if res is None:
            return None
        value, next_idx = res
        return value, next_idx
    if p == ord(b"$"):
        res = _parse_bulk_string(data, idx)
        if res is None:
            return None
        value, next_idx = res
        return value, next_idx
    if p == ord(b"*"):
        res = _parse_int_line(data, idx)
        if res is None:
            return None
        count, idx = res
        if count == -1:
            return None, idx  # Null array
        elements = []
        for _ in range(count):
            sub = _parse_frame_at(data, idx)
            if sub is None:
                return None
            element, idx = sub
            elements.append(element)
        return elements, idx
    # Unsupported/unknown
    return None


def _encode_simple_string(s: bytes) -> bytes:
    return b"+" + s + b"\r\n"


def _encode_bulk_string(b: bytes | None) -> bytes:
    if b is None:
        return b"$-1\r\n"
    return b"$" + str(len(b)).encode() + b"\r\n" + b + b"\r\n"


def _encode_array(elements: list[bytes]) -> bytes:
    # RESP arrays: *<len>\r\n then each element as bulk string
    count = len(elements)
    if count == 0:
        return b"*0\r\n"
    parts = [b"*" + str(count).encode() + b"\r\n"]
    for e in elements:
        parts.append(_encode_bulk_string(e))
    return b"".join(parts)


def _encode_integer(n: int) -> bytes:
    return b":" + str(n).encode() + b"\r\n"


def _encode_null_array() -> bytes:
    return b"*-1\r\n"


def _consume_next_frame(buffer: bytearray):
    """Parse and consume a single RESP frame from the buffer if available.

    Returns the parsed frame, or None if the buffer does not yet contain a full frame.
    """
    parsed = _parse_frame_at(buffer, 0)
    if parsed is None:
        return None
    frame, next_idx = parsed
    if next_idx > 0:
        del buffer[:next_idx]
    return frame


def _handle_ping(connection: socket.socket) -> None:
    connection.sendall(_encode_simple_string(b"PONG"))


def _handle_echo(connection: socket.socket, array: list) -> None:
    arg = array[1] if len(array) >= 2 else None
    if isinstance(arg, (bytes, bytearray)):
        connection.sendall(_encode_bulk_string(bytes(arg)))
    else:
        connection.sendall(_encode_bulk_string(None))


def _parse_set_expiry_ms(array: list, start_index: int = 3) -> int | None:
    """Parse PX/EX options for SET and return expiry in milliseconds if provided.

    The function is tolerant of unknown/invalid options and values, and will
    consider the last valid PX/EX option seen.
    """
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

    # After responding to pusher, try waking BLPOP waiters (one per pushed element)
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
    # Per stage, timeout will be 0 (block indefinitely). Parse but ignore non-zero.
    try:
        timeout = int(
            bytes(timeout_raw)
            if isinstance(timeout_raw, (bytes, bytearray))
            else timeout_raw
        )
    except Exception:
        timeout = 0

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
    # For non-zero timeouts (future stages), we could wait with timeout
    signaled = event.wait(timeout=max(0, timeout))
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


def _process_frame(connection: socket.socket, frame) -> None:
    """Process a single RESP frame and respond if it's a recognized command."""
    if isinstance(frame, list):
        _dispatch_array_command(connection, frame)


def handle_client(connection: socket.socket) -> None:
    buffer = bytearray()
    try:
        while True:
            chunk = connection.recv(4096)
            if not chunk:
                break
            buffer.extend(chunk)

            # Try to parse as many frames as available in buffer
            while True:
                frame = _consume_next_frame(buffer)
                if frame is None:
                    break
                _process_frame(connection, frame)
                # Unrecognized frames are ignored to keep the server minimal
    finally:
        connection.close()


def main():
    print("Logs from your program will appear here!")

    server_socket = socket.create_server(("localhost", 6379), reuse_port=True)
    while True:
        connection, _ = server_socket.accept()  # wait for client
        thread = threading.Thread(target=handle_client, args=(connection,), daemon=True)
        thread.start()


if __name__ == "__main__":
    main()
