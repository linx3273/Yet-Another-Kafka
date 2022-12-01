import sys
import threading
from http.server import HTTPServer, BaseHTTPRequestHandler
import constants
import requests


class Consumer:
    def __init__(self, zoo_addr, topic, typ="register"):
        self.zoo_addr = zoo_addr
        self.leader_addr = None
        self.addr = None
        self.topic = topic
        self.type = typ

    def set_port(self, port):
        self.addr = port

    def query_zoo(self):
        inc = constants.to_json(frm="consumer", port=self.addr)
        r = requests.post(f"{constants.LOCALHOST}:{self.zoo_addr}", data=inc)

    def set_leader_addr(self, port):
        self.leader_addr = port
        print(f"Leader - {self.leader_addr}")

    def register_to_leader(self):
        print(f"Registering to leader")
        inc = constants.to_json(frm="consumer", port=self.addr, typ=self.type, topic=self.topic)
        r = requests.post(f"{constants.LOCALHOST}:{self.leader_addr}", data=inc)

    def disconnect(self):
        try:
            if input(">") == "disconnect":
                inc = constants.to_json(frm="consumer", port=self.addr, typ="disconnect")
                r = requests.post(f"{constants.LOCALHOST}:{self.leader_addr}", data=inc)
                print("Disconnected")
                sys.exit()
        except KeyboardInterrupt as e:
            print(e)

class RequestHandler(BaseHTTPRequestHandler):
    global consumer

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def do_GET(self):
        self.send_response(200)
        self.send_header('Content-type', 'text/plain')
        self.end_headers()
        self.wfile.write(bytes("Server Active", "utf-8"))

    def do_POST(self):
        inc = constants.to_dict(
            self.rfile.read(int(self.headers['Content-Length'])).decode('utf-8')
        )

        self.send_response(200)
        self.send_header('Content-type', 'application/json')
        self.end_headers()
        self.wfile.write(bytes("Server Active", "utf-8"))


        if inc['from'] == "zookeeper" and inc['type'] == "set-leader":
            consumer.set_leader_addr(inc['data'])
            consumer.register_to_leader()

        if inc['from'] == 'broker':
            if inc['topic'] == consumer.topic:
                print(inc['data'])


if __name__ == "__main__":
    consumer = Consumer(
                            constants.ZOOKEEPER_PORT,
                            input("Enter topic name: "),
                            typ=input("Choose between 'register/from-beginning': ")
                        )

    server = HTTPServer(("localhost", 0), RequestHandler)
    print(f"Listening on {server.server_address[0]}:{server.server_address[1]}")
    consumer.set_port(server.server_address[1])

    p = threading.Thread(target=server.serve_forever)
    p.daemon = True
    p.start()

    consumer.query_zoo()
    consumer.disconnect()
