import socket
import threading

# In-memory key-value store for SET/GET
_kv_store: dict[bytes, bytes] = {}
_kv_lock = threading.Lock()


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
        with _kv_lock:
            _kv_store[key] = value
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
            value = _kv_store.get(key)
        connection.sendall(_encode_bulk_string(value if value is not None else None))
    else:
        connection.sendall(_encode_bulk_string(None))


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
