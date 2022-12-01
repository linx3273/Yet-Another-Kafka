import sys
import threading
import time
from http.server import HTTPServer, BaseHTTPRequestHandler
import constants
import requests


class Producer:
    def __init__(self, zoo_addr, topic):
        self.zoo_addr = zoo_addr
        self.leader_addr = None
        self.addr = None
        self.topic = topic

    def set_port(self, port):
        self.addr = port

    def query_zoo(self):
        inc = constants.to_json(frm="producer", port=self.addr)
        r = requests.post(f"{constants.LOCALHOST}:{self.zoo_addr}", data=inc)

    def set_leader_addr(self, port):
        self.leader_addr = port
        print(f"Leader - {self.leader_addr}")

    def register_to_leader(self):
        print("Resgistering with leader")
        inc = constants.to_json(frm="producer", port=self.addr, typ="register", topic=self.topic)
        r = requests.post(f"{constants.LOCALHOST}:{self.leader_addr}", data=inc)

    def publish_data(self):
        while True:
            try:
                inp = input("> ")
                if inp == "disconnect":
                    inc = constants.to_json(frm="producer", port=self.addr, typ="disconnect")
                    r = requests.post(f"{constants.LOCALHOST}:{self.zoo_addr}", data=inc)
                    print("Disconnected")
                    sys.exit()
                else:
                    inc = constants.to_json(frm="producer", port=self.addr, typ="publish", data=inp, topic=self.topic)
                    r = requests.post(f"{constants.LOCALHOST}:{self.leader_addr}", data=inc)
            except KeyboardInterrupt as e:
                print(e)
                break


class RequestHandler(BaseHTTPRequestHandler):
    global producer

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
        self.send_header('Content-type', 'text/plain')
        self.end_headers()
        self.wfile.write(bytes("Server Active", "utf-8"))

        if inc["from"] == "zookeeper" and inc["type"] == "set-leader":
            producer.set_leader_addr(inc["data"])
            producer.register_to_leader()


def run():
    global producer
    time.sleep(1)
    producer.query_zoo()
    producer.publish_data()


if __name__ == "__main__":
    producer = Producer(
                            constants.ZOOKEEPER_PORT,
                            input("Enter topic name: ")
                        )

    server = HTTPServer(("localhost", 0), RequestHandler)
    print(f"Listening on {server.server_address[0]}:{server.server_address[1]}")
    producer.set_port(server.server_address[1])

    p = threading.Thread(target=server.serve_forever)
    p.daemon = True
    p.start()

    producer.query_zoo()
    producer.publish_data()


