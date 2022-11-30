import threading
from http.server import HTTPServer, BaseHTTPRequestHandler
import constants
import time
import requests
from pathlib import Path
import os
import sys
import subprocess

BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(sys.argv[0]), os.pardir))


class Zookeeper(BaseHTTPRequestHandler):
    def __init__(self, *args, **kwargs):
        self.brokers = {
                            constants.BROKER_PORT[0]: constants.TIME_LIMIT,
                            constants.BROKER_PORT[1]: constants.TIME_LIMIT,
                            constants.BROKER_PORT[2]: constants.TIME_LIMIT
                        }
        self.leader = None  # holds port of leader node

        self.run()

        super().__init__(*args, **kwargs)

    def start_all_brokers(self):
        """
        Method to call start_broker multiple times and thus spawn the brokers
        :return:
        """
        print("Starting brokers")
        leader = self.elect_leader()
        for i in self.brokers:
            if leader == i:
                # if current port obtained from loop matches chosen leader port; set leader bit to 1
                self.spawn_broker(i, 1)
            else:
                # current port obtained from loop does not match chosen leader port; set leader bit to 0
                self.spawn_broker(i, 0)
        print("Started all brokers")

    def spawn_broker(self, port, leader):
        """
        Method to spawn one Broker
        :param port: Port Number that will be used by the broker
        :param leader: If 1, the spawned broker is a leader, 0 for not leader
        :return:
        """
        print(f"Starting broker with port {port}")
        broker = Path(BASE_DIR + '\\src\\broker.py').resolve().as_posix()

        p = threading.Thread(target=subprocess.call, args=[f"python {broker} {leader} {port}"], kwargs={"Shell": True})
        p.daemon = True     # if zookeeper dies, the processes are killed
        p.start()

        print("Done")

    def monitor_heartbeat(self, port):
        """
        Will decrement the self.broker values for each port. If it reaches zero it'll assume that the broker has died
        and will attempt to re-start it
        :param port: Will monitor a broker using the particular PORT
        :return:
        """
        while True:
            if self.brokers[port] == 0:
            # countdown of broker reached 0 assuming it has crashed, will attempt to spawn new broker

                if port == list(self.brokers.keys())[0]:
                    # leader broker has died, so elect new leader
                    # del self.brokers[port]
                    # self.brokers[port] = constants.TIME_LIMIT

                    # lead = self.elect_leader()
                    # send post request to chosen broker and inform it to become leader and restart the dead broker
                    # TODO
                    pass
                else:
                    # non leader broker has died, spawn new broker in its place
                    # self.spawn_broker(port, 0)
                    pass
                print(f"{port} Died")

            time.sleep(1)
            self.brokers[port] -= 1

    def elect_leader(self):
        """
        Returns the first key in the dictionary
        :return: PORT Number of broker
        """
        return list(self.brokers.keys())[0]

    def change_leader(self):
        """
        Will send instruction to an active broker to become the leader
        :return:
        """
        # TODO
        r = requests.post()

    def do_POST(self):
        """
        Handles all the POST requests sent to the Zookeeper and manages them accordingly
        :return:
        """
        inc = self.rfile.read(int(self.headers['Content-Length'])).decode('utf-8')

        if inc == f"//{constants.BROKER_PORT[0]}//":   # monitoring broker with PORT 8001
            print("Pulse from 8001")
            self.brokers[constants.BROKER_PORT[0]] = constants.TIME_LIMIT

        elif inc == f"//{constants.BROKER_PORT[0]}//":     # monitoring broker with PORT 8002
            print("Pulse from 8002")
            self.brokers[constants.BROKER_PORT[1]] = constants.TIME_LIMIT

        elif inc == f"//{constants.BROKER_PORT[0]}//":     # monitoring broker with PORT 8003
            print("Pulse from 8003")
            self.brokers[constants.BROKER_PORT[2]] = constants.TIME_LIMIT

        elif "//producer//" in inc:     # obtained message from producer
            # TODO
            pass

        elif "//consumer//" in inc:     # detected a new consumer, re-route it to leader broker
            # TODO
            r = requests.post()

    def do_GET(self):
        pass

    def run(self):
        """
        Handles all method callbacks to handle all launches during __init__ phase including the threads
        :return:
        """
        # self.start_all_brokers()

        t = []
        for i in constants.BROKER_PORT:
            t.append(
                threading.Thread(target=self.monitor_heartbeat, args=[i])
            )

        for i in t:
            i.daemon = True
            i.start()


if __name__ == "__main__":
    server = HTTPServer(('localhost', constants.ZOOKEEPER_PORT), Zookeeper)
    print(f"Server running on {server.server_address[0]}:{server.server_address[1]}")
    server.serve_forever()
