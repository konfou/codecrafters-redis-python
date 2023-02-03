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
    lock = threading.Lock()

    def __init__(self):
        self.store = {}

    # XXX: maybe move those to RESP class
    def resp_err_arity(self, cmd):
        return b"-ERR wrong number of arguments for '%b' command\r\n" % cmd

    def resp_bulk_string(self, string):
        return b'$%d\r\n%b\r\n' % (len(string), string)

    def store_set(self, args):
        cmd = b'set'

        if args is None or len(args) != 2:
            ret = self.resp_err_arity(cmd)
        else:
            self.store[args[0]] = args[1]
            ret = b'+OK\r\n'

        return ret

    def store_get(self, args):
        cmd = b'get'

        if args is None or len(args) != 1:
            ret = self.resp_err_arity(cmd)
        elif args[0] in self.store:
            value = self.store[args[0]]
            ret = self.resp_bulk_string(value)
        else:
            ret = b'$-1\r\n'

        return ret

    def run(self, cmd, args=None):
        # XXX: maybe let default args be [] so don't have to check both
        # whether is None and args len
        if isinstance(cmd, list):
            cmd, *args = cmd

        if cmd.upper() == b'PING':
            if args is None or len(args) == 0:
                ret = b'+PONG\r\n'
            elif len(args) == 1:
                ret = self.resp_bulk_string(args[0])
            else:
                ret = self.resp_err_arity(cmd)
        elif cmd.upper() == b'ECHO':
            if args is None or len(args) != 1:
                ret = self.resp_err_arity(cmd)
            else:
                ret = self.resp_bulk_string(args[0])
        elif cmd.upper() == b'SET':
            with Store.lock:
                ret = self.store_set(args)
        elif cmd.upper() == b'GET':
            ret = self.store_get(args)
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
