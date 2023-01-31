import socket
import threading


class ClientThread(threading.Thread):
    def __init__(self, conn):
        threading.Thread.__init__(self)
        self.conn = conn

    def run(self):
        while True:
            data = self.conn.recv(1024)
            if not data:
                break
            self.conn.send("+PONG\r\n".encode())


def main():
    # You can use print statements as follows for debugging, they'll be visible when running tests.
    print("Logs from your program will appear here!")

    server_socket = socket.create_server(("localhost", 6379), reuse_port=True)
    while True:
        conn, _ = server_socket.accept()
        new_thread = ClientThread(conn)
        new_thread.start()


if __name__ == "__main__":
    main()
