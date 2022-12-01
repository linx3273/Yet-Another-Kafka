import threading
from http.server import HTTPServer, BaseHTTPRequestHandler
import constants
import time
import requests
from pathlib import Path
import os
import sys
import subprocess
import multiprocessing
from sys import platform


BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(sys.argv[0]), os.pardir))


class ZooKeeper:
    def __init__(self):
        self.brokers = {
                            constants.BROKER_PORT[0]: constants.TIME_LIMIT,
                            constants.BROKER_PORT[1]: constants.TIME_LIMIT,
                            constants.BROKER_PORT[2]: constants.TIME_LIMIT
                        }
        self.port = constants.ZOOKEEPER_PORT
        self.leader = self.elect_leader()
        self.producers = []  # holds a list of all producers

    def start_all_brokers(self):
        """
        Method to call start_broker multiple times and thus spawn the brokers
        :return:
        """
        print("Starting brokers")
        leader = self.leader
        for i in self.brokers:
            if leader == i:
                # if current port obtained from loop matches chosen leader port; set leader bit to 1
                self.spawn_broker(i, 1)
            else:
                # current port obtained from loop does not match chosen leader port; set leader bit to 0
                self.spawn_broker(i, 0)
        print("Started all brokers")

    @staticmethod
    def spawn_broker(port, leader):
        """
        Method to spawn one Broker using multiprocessing library
        :param port: Port Number that will be used by the broker
        :param leader: If 1, the spawned broker is a leader, 0 for not leader
        :return:
        """
        print(f"Starting broker with port {port}")
        broker = Path(BASE_DIR + '\\src\\broker.py').resolve().as_posix()

        if platform == "linux" or platform == "linux2":
            p = multiprocessing.Process(
                                            target=subprocess.call,
                                            args=[f"python {broker} {leader} {port}"],
                                            kwargs={"shell": True, "creationflags": subprocess.CREATE_NEW_CONSOLE}
                                        )
        elif platform == "win32":
            p = multiprocessing.Process(
                                        target=os.system,
                                        args=[f"start cmd /k python {broker} {leader} {port}"]
                                    )
        p.daemon = True    # if zookeeper dies, the processes are killed
        p.start()

    def monitor_heartbeat(self, port):
        """
        Will decrement the broker values for each port. If it reaches zero it'll assume that the broker has died
        and will attempt to re-start it
        :param port: Will monitor a broker using the particular PORT
        :return:
        """
        while True:
            if self.brokers[port] == 0:
                print(f"Broker Died -- {port}")
                # countdown of broker reached 0 assuming it has crashed, will attempt to spawn new broker

                if port == list(self.brokers.keys())[0]:
                    print(f"Leader broker died -- {port} -- {self.leader}")
                    # leader broker has died, so elect new leader
                    del self.brokers[port]
                    self.brokers[port] = constants.TIME_LIMIT

                    self.leader = self.elect_leader()
                    print(f"Picked new leader -- {self.leader}")
                    # send post request to chosen broker and inform it to become leader and restart the dead broker
                    inc = constants.to_json(frm="zookeeper", typ="set-leader", port=self.port, data=self.leader)
                    r = requests.post(f"{constants.LOCALHOST}:{self.leader}", data=inc)

                    self.inform_producers()     # informs producers of the newly elected leader broker
                    # respawn dead broker as non leader
                    self.spawn_broker(port, 0)

                else:
                    # non leader broker has died, spawn new broker in its place
                    self.spawn_broker(port, 0)

                # inform the leader that a new broker has been created
                inc = constants.to_json(frm="zookeeper", typ="sync", port=self.port, data=port)
                r = requests.post(f"{constants.LOCALHOST}:{self.leader}", data=inc)

            time.sleep(constants.INTERVALS)
            self.brokers[port] -= constants.INTERVALS

    def inform_producers(self):
        """
        Inform all producers of the update leader
        :return:
        """
        print("Informing procedures of new leader")
        for i in self.producers:
            try:
                inf = constants.to_json(frm="zookeeper", typ="set-leader", port=self.port, data=self.leader)
                r = requests.post(f"{constants.LOCALHOST}:{i}", data=inf)
            except:
                print(f"Could not inform producer with port - {i}. Assuming it has died. Removing it from list of register producers")
                self.producers.remove(i)

    def elect_leader(self):
        """
        Returns the first key in the dictionary
        :return: PORT Number of broker
        """
        return list(self.brokers.keys())[0]

    def run(self):
        """
        Handles all method callbacks to handle all launches during __init__ phase including the threads
        :return:
        """
        self.start_all_brokers()

        # creating threads to monitor all brokers
        t = []
        for i in constants.BROKER_PORT:
            t.append(
                threading.Thread(target=self.monitor_heartbeat, args=[i])
            )

        for i in t:
            i.daemon = True
            i.start()


class RequestHandler(BaseHTTPRequestHandler):
    global zookeeper

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def do_GET(self):
        """
        Handles all the GET requests sent to the Zookeeper and manages them accordingly
        :return:
        """
        self.send_response(200)
        self.send_header('Content-type', 'text/plain')
        self.end_headers()
        self.wfile.write(bytes("Server Active", "utf-8"))

    def do_POST(self):
        """
        Handles all the POST requests sent to the Zookeeper and manages them accordingly
        :return:
        """
        inc = constants.to_dict(
            self.rfile.read(int(self.headers['Content-Length'])).decode('utf-8')
        )
        self.send_response(200)
        self.send_header('Content-type', 'application/json')
        self.end_headers()

        if inc["from"] == "broker" and inc["port"] == constants.BROKER_PORT[0]:  # monitoring broker with PORT 8001
            zookeeper.brokers[constants.BROKER_PORT[0]] = constants.TIME_LIMIT

        elif inc["from"] == "broker" and inc["port"] == constants.BROKER_PORT[1]:  # monitoring broker with PORT 8002
            zookeeper.brokers[constants.BROKER_PORT[1]] = constants.TIME_LIMIT

        elif inc["from"] == "broker" and inc["port"] == constants.BROKER_PORT[2]:  # monitoring broker with PORT 8003
            zookeeper.brokers[constants.BROKER_PORT[2]] = constants.TIME_LIMIT

        elif inc["from"] == "producer":  # provide producer info about leader broker
            if inc["type"] == "disconnect":
                print(f"Producer {inc['port']} disconnected. Removing from list of registered producers")
                zookeeper.producers.remove(inc["port"])
            else:
                # try:
                print(f"New producer has connected - {inc['port']}. Registering it")
                zookeeper.producers.append(inc["port"])
                inf = constants.to_json(frm="zookeeper", typ="set-leader", port=zookeeper.port, data=zookeeper.leader)
                r = requests.post(f"{constants.LOCALHOST}:{inc['port']}", data=inf)
                # except:
                #     print(f"Could not communicate with producer - {inc['port']} . Assuming it has died. Removing it from registered producers")
                #     zookeeper.producers.remove(inc["port"])

        elif inc["from"] == "consumer":  # detected a new consumer, re-route it to leader broker
            print(f"New consumer has connected - {inc['port']}")
            inf = constants.to_json(frm="zookeeper", typ="set-leader", port=zookeeper.port, data=zookeeper.leader)
            r = requests.post(f"{constants.LOCALHOST}:{inc['port']}", data=inf)


if __name__ == "__main__":
    zookeeper = ZooKeeper()
    zookeeper.run()

    server = HTTPServer(('localhost', constants.ZOOKEEPER_PORT), RequestHandler)
    print(f"Server running on {server.server_address[0]}:{server.server_address[1]}")
    server.serve_forever()
