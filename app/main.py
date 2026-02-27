import socket
import threading

from resp import _consume_next_frame
from commands import _process_frame


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
        thread = threading.Thread(
            target=handle_client,
            args=(connection,),
            daemon=True,
        )
        thread.start()


if __name__ == "__main__":
    main()
