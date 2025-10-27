import socket
import threading


def handle_client(connection: socket.socket) -> None:
    try:
        while True:
            message = connection.recv(1024)
            if not message:
                break
            # Respond once for every PING received in this chunk
            ping_count = message.count(b"PING")
            for _ in range(ping_count):
                connection.sendall(b"+PONG\r\n")
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
