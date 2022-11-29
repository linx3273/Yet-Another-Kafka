import os
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
        self.addr = addr  # will hold the address of the HTTP server

        self.topics = {}  # holds topic name and file pointers
        self.brokers = []  # holds a list of addresses of other brokers
        self.producers = []  # holds a list of addresses of all producers

        self.topic_dir = Path(BASE_DIR + '\\topics').resolve().as_posix()
        self.src_dir = Path(BASE_DIR + '\\src').resolve().as_posix()
        self.log_dir = Path(BASE_DIR + '\\logs').resolve().as_posix()

        self.shutdown = False  # when set true the broker will stop processes

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
            if self.leader:
                r = requests.post(self.zoo_addr, data=f"//{self.addr}//")
                time.sleep(5)

    def do_POST(self):
        """
        Will handle all post requests that are sent to the HTTP Server (in this case all details sent do it by the
        producers
        :return:
        """
        print(self.rfile.read(int(self.headers['Content-Length'])).decode('utf-8'))
        # TODO


if __name__ == "__main__":
    lead = bool(int(sys.argv[1]))   # leader
    index = int(sys.argv[2])    # index for to select one of the three preset ports

    handler = (Broker, constants.ZOOKEEPER_PORT, lead, constants.BROKER_PORT[index])
    server = HTTPServer(('localhost', constants.BROKER_PORT[index]), handler)
    print(f"Server running on PORT - {server.server_address[1]}")
    server.serve_forever()
