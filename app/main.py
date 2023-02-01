import socket
import threading


class ClientThread(threading.Thread):
    def __init__(self, conn):
        threading.Thread.__init__(self)
        self.conn = conn

    def run(self):
        resp = RESP()
        while True:
            data = self.conn.recv(1024)
            if not data:
                break
            resp.buf = data
            self.conn.send(store.run(resp.parse()))


class RESP:
    def __init__(self):
        self.buf = b''

    def read(self, bufsize=None):
        if bufsize is None:
            # ref:redis: In RESP, different parts of the protocol are
            # always terminated with "\r\n" (CRLF).
            ret, _, self.buf = self.buf.partition(b'\r\n')
        else:
            ret, self.buf = self.buf[:bufsize], self.buf[bufsize:]

        return ret

    def parse(self):
        # ref:redis: In RESP, the first byte determines the data type.
        first_byte = self.read(1)
        if first_byte == b'+':  # ref: Simple Strings
            ret = self.read()
        elif first_byte == b'$':  # ref: Bulk Strings
            self.read()
            ret = self.read()
        elif first_byte == b'*':  # ref: Arrays
            num = int(self.read())
            ret = [self.parse() for _ in range(num)]
        else:
            raise Exception("No such data type.")

        return ret


class Store:
    def __init__(self):
        pass

    def err_args_num(self, cmd):
        return b"-ERR wrong number of arguments for '%b' command\r\n" % cmd

    def run(self, cmd, args=None):
        if isinstance(cmd, list):
            cmd, *args = cmd

        if cmd.upper() == b'PING':
            if args is None or len(args) == 0:
                ret = b'+PONG\r\n'
            elif len(args) == 1:
                ret = b'$%d\r\n%b\r\n' % (len(args[0]), args[0])
            else:
                ret = self.err_args_num(cmd)
        elif cmd.upper() == b'ECHO':
            if args is None or len(args) != 1:
                ret = self.err_args_num(cmd)
            else:
                ret = b'$%d\r\n%b\r\n' % (len(args[0]), args[0])
        else:
            ret = b"-ERR unknown command '%s'" % cmd

        return ret


store = Store()


def main():
    # You can use print statements as follows for debugging, they'll be visible when running tests.
    print("Logs from your program will appear here!")

    server_socket = socket.create_server(("localhost", 6379), reuse_port=True)
    while True:
        conn, _ = server_socket.accept()
        new_thread = ClientThread(conn)
        new_thread.start()


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print('Interrupted.')
