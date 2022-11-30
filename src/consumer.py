import sys
import threading
from http.server import HTTPServer, BaseHTTPRequestHandler
import constants
import requests
import time

class Consumer:
    def __init__(self, zoo_addr, topic, typ="register"):
        self.zoo_addr = "http://" + zoo_addr[0] + ":" + zoo_addr[1]
        self.leader_addr = None
        self.addr = None
        self.topic = topic
        self.type = typ

    def set_port(self, port):
        self.addr = port

    def query_zoo(self):
        inc = constants.to_json(frm="consumer", port=self.addr)
        try:
            r = requests.post(f"{self.zoo_addr}", data=inc)
        except Exception as e:
            print("query zoo")
            print(e)

    def set_leader_addr(self, port):
        self.leader_addr = constants.LOCALHOST + f":{port}"

    def register_to_leader(self):
        inc = constants.to_json(frm="consumer", port=self.addr, typ=self.type, topic=self.topic)
        try:
            r = requests.post(f"{self.leader_addr}", data=inc)
        except Exception as e:
            print("reg to lead")
            print(e)

    def stop_condition(self):
        if input(">") == "stop":
            sys.exit()


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
    consumer = Consumer(['127.0.0.1', '8000'], 'test', typ="from-beginning")

    server = HTTPServer(("localhost", 0), RequestHandler)
    print(f"Listening on {server.server_address[0]}:{server.server_address[1]}")
    consumer.set_port(server.server_address[1])

    p = threading.Thread(target=server.serve_forever)
    p.daemon = True
    p.start()

    consumer.query_zoo()
    consumer.stop_condition()
