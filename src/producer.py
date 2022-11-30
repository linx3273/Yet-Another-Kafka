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
        try:
            r = requests.post(f"{constants.LOCALHOST}:{self.zoo_addr}", data=inc)
        except Exception as e:
            print("queryzoo")
            print(e)

    def set_leader_addr(self, port):
        self.leader_addr = port

    def register_to_leader(self):
        inc = constants.to_json(frm="producer", port=self.addr, typ="register", topic=self.topic)
        try:
            print(f"{constants.LOCALHOST}:{self.leader_addr}")
            r = requests.post(f"{constants.LOCALHOST}:{self.leader_addr}", data=inc)
        except Exception as e:
            print("reg to lead")
            print(e)

    def publish_data(self):
        while True:
            try:
                inp = input("> ")
                inc = constants.to_json(frm="producer", port=self.addr, typ="publish", data=inp, topic=self.topic)
                try:
                    r = requests.post(f"{constants.LOCALHOST}:{self.leader_addr}", data=inc)
                except Exception as e:
                    print(e)
            except KeyboardInterrupt:
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


