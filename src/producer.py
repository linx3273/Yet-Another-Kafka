import threading
from http.server import HTTPServer, BaseHTTPRequestHandler
import constants
import requests


class Producer:
    def __init__(self, zoo_addr, topic):
        self.zoo_addr = "http://" + zoo_addr[0] + ":" + zoo_addr[1]
        self.leader_addr = None
        self.addr = None
        self.topic = topic

    def set_port(self, port):
        self.zoo_addr = port

    def query_zoo(self):
        inc = constants.to_json(frm="producer", port=self.addr)
        r = requests.post(f"{self.zoo_addr}", data=inc)

    def set_leader_addr(self, port):
        self.leader_addr = constants.LOCALHOST + f":{port}"

    def register_to_leader(self):
        inc = constants.to_json(frm="producer", port=self.addr, typ="register", topic=self.topic)
        r = requests.post(f"{self.leader_addr}", data=inc)

    def publish_data(self):
        while True:
            try:
                inp = input("> ")
                inc = constants.to_json(frm="producer", port=self.addr, typ="publish", data=inp, topic=self.topic)
                r = requests.post(f"{self.leader_addr}", data=inc)
            except KeyboardInterrupt:
                break


class RequestHandler(BaseHTTPRequestHandler):
    global producer

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def do_POST(self):
        inc = constants.to_dict(
            self.rfile.read(int(self.headers['Content-Length'])).decode('utf-8')
        )

        if inc["from"] == "zookeeper" and inc["type"] == "set-leader":
            producer.set_leader_port(inc["data"])
            producer.register_to_leader()


def run():
    global producer

    producer.query_zoo()
    producer.publish_data()


if __name__ == "__main__":
    producer = Producer(["127.0.0.1", "8000"], "test")

    server = HTTPServer((constants.LOCALHOST, 0), RequestHandler)
    print(f"Listening on {server.server_address[0]}:{server.server_address[1]}")

    producer.set_port(server.server_address[1])
    p = threading.Thread(target=run)
    p.daemon = True
    p.start()

    server.serve_forever()

