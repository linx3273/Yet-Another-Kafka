import os
import threading
import time
from pathlib import Path
import sys
from http.server import HTTPServer, BaseHTTPRequestHandler
import requests
import constants


BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(sys.argv[0]), os.pardir))


class Broker(BaseHTTPRequestHandler):
    def __init__(self, zoo_addr, leader, addr, *args, **kwargs):
        self.zoo_addr = zoo_addr  # stores the port number that the zookeeper is hosted on
        self.leader = leader  # True if leader, else False
        self.addr = addr  # will hold its own PORT

        self.topics = {}  # holds topic name and file pointers
        self.producers = []  # holds a list of addresses of all producers

        self.brokers = constants.BROKER_PORT
        try:
            self.brokers.remove(self.addr)  # Stores the ports of other brokers
        except Exception as e:
            print(e)

        self.topic_dir = Path(BASE_DIR + '\\topics').resolve().as_posix()
        self.src_dir = Path(BASE_DIR + '\\src').resolve().as_posix()
        self.log_dir = Path(BASE_DIR + '\\logs').resolve().as_posix()

        self.shutdown = False  # when set true the broker will stop processes

        self.heartbeat()

        super().__init__(*args, **kwargs)

    def query_topics(self):
        """
        Generates a dictionary of the existing topics and gets access to its file partitions
        :return:
        """
        if os.path.exists(self.topic_dir):
            for root, subdirs, files in os.walk(self.topic_dir):
                for subdir in subdirs:
                    self.topics[subdir] = []
                if root != self.topic_dir:
                    for file in files:
                        topic_name = Path(root).name
                        file_dir = Path(self.topic_dir + f'\\{topic_name}\\{file}').resolve().as_posix()
                        file_pointer = open(file_dir, 'a')
                        self.topics[topic_name].append(file_pointer)
        else:
            os.mkdir(self.topic_dir)

    def create_topic(self, topic_name, partition_count=3):
        """
        Creates a new topic given the name and adds it to topic tracker dictionary
        :param topic_name: Name of the topic that is to be created
        :param partition_count: Number of partitions for the given topic_name
        :return:
        """
        if topic_name not in self.topics:
            topic_dir = Path(self.topic_dir + f'\\{topic_name}').resolve().as_posix()
            os.mkdir(topic_dir)
            self.topics[topic_name] = []

            for i in range(partition_count):
                file_dir = Path(topic_dir + f'\\{i}').resolve().as_posix()
                file_pointer = open(file_dir, 'a')
                self.topics[topic_name].append(file_pointer)

    def heartbeat(self):
        """
        Pings the zookeeper every five seconds
        :return:
        """
        while not self.shutdown:
            try:
                time.sleep(constants.INTERVALS)
                inf = constants.to_json(frm="broker", port=self.addr, typ="pulse")
                r = requests.post(f"{constants.LOCALHOST}:{self.zoo_addr}", data=inf)
            except:
                pass

    def start_threads(self):
        """
        Will start all the threads required for communication
        :return:
        """

        t = []
        t.append(threading.Thread(target=self.heartbeat()))

        for i in t:
            i.daemon = True
            i.start()


class RequestHandler(BaseHTTPRequestHandler):
    global broker

    def __init__(self, *args, **kwargs):
        super().__init__()

    def do_GET(self):
        pass

    def do_POST(self):
        """
        Will handle all post requests that are sent to the HTTP Server (in this case all details sent do it by the
        producers
        :return:
        """
        inc = self.rfile.read(int(self.headers['Content-Length'])).decode('utf-8')

        if "//register//consumer//" in inc:     # a new consumer wants to join; extract the port number and save it
            pass

        elif inc == constants.SET_LEADER:   # leader node has died to zookeeper is instructing this broker to become leader
            broker.leader = 1

        elif "//producer//" in inc:     # data sent by producer; process it and send to other nodes
            pass
            if broker.leader:
                # forward data to other brokers as well
                pass

        elif "//update//" in inc:   # add data sent by leader node to queue
            pass

        elif "//pop-top//" in inc:      # remove the front element in the queue
            pass
        # TODO


if __name__ == "__main__":
    lead = bool(int(sys.argv[1]))   # leader
    port = int(sys.argv[2])    # index for to select one of the three preset ports

    broker = Broker(constants.ZOOKEEPER_PORT, lead, port)

    server = HTTPServer(('localhost', port), RequestHandler)
    print(f"Server running on PORT - {server.server_address[0]}:{server.server_address[1]}")
    server.serve_forever()
