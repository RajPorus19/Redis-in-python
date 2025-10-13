import socket  # noqa: F401


def main():
    print("Logs from your program will appear here!")

    server_socket = socket.create_server(("localhost", 6379), reuse_port=True)
    while True:
        connection, _ = server_socket.accept()  # wait for client
        message = connection.recv(1024)
        ping_count = message.count(b"PING")
        return_message = b"+" + b"PONG" * ping_count + b"\r\n"
        connection.sendall(return_message)
        connection.close()


if __name__ == "__main__":
    main()
