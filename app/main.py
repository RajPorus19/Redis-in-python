import socket
import threading


def _find_crlf(data: bytearray, start: int) -> int:
    idx = data.find(b"\r\n", start)
    return idx


def _parse_simple_string(data: bytearray, start: int):
    # +<string>\r\n
    end = _find_crlf(data, start)
    if end == -1:
        return None
    return bytes(data[start:end]), end + 2


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


def _dispatch_array_command(connection: socket.socket, array: list) -> None:
    """Handle RESP Array-based commands like PING and ECHO."""
    if not array:
        return
    cmd_raw = array[0]
    if not isinstance(cmd_raw, (bytes, bytearray)):
        return
    cmd = bytes(cmd_raw).upper()

    if cmd == b"PING":
        connection.sendall(_encode_simple_string(b"PONG"))
        return

    if cmd == b"ECHO" and len(array) >= 2:
        arg = array[1]
        if isinstance(arg, (bytes, bytearray)):
            connection.sendall(_encode_bulk_string(bytes(arg)))
        else:
            connection.sendall(_encode_bulk_string(None))
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
