import datetime
import json
import multiprocessing
import operator
import os
import threading
import time
from pathlib import Path
import sys
from http.server import HTTPServer, BaseHTTPRequestHandler
import requests
import constants


BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(sys.argv[0]), os.pardir))


class Broker:
    def __init__(self, zoo_addr, leader, addr):
        self.zoo_addr = zoo_addr  # stores the port number that the zookeeper is hosted on
        self.leader = leader  # True if leader, else False
        self.addr = addr  # will hold its own PORT

        self.topics = {}  # holds topic name and file pointers
        self.producers = []  # holds a list of addresses of all producers
        self.consumers = []

        self.brokers = constants.BROKER_PORT
        self.brokers.remove(self.addr)  # Stores the ports of other brokers

        self.topic_dir = Path(BASE_DIR + '\\topics').resolve().as_posix()
        self.src_dir = Path(BASE_DIR + '\\src').resolve().as_posix()
        self.log_dir = Path(BASE_DIR + '\\logs').resolve().as_posix()

        self.shutdown = False  # when set true the broker will stop processes

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
                        self.topics[topic_name].append(file_dir)
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
                open(file_dir, "w").close()
                self.topics[topic_name].append(file_dir)

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

    def start_heartbeat(self):
        """
        Will start all the threads required for communication
        :return:
        """
        t = threading.Thread(target=self.heartbeat())
        t.daemon = True
        t.start()

    def send_sync_data(self):
        """
        Broker will generate its metadata as a message and forward to other brokers
        :return:
        """
        data = {
            "producers": self.producers,
            "consumers": self.consumers,
            "topics": self.topics
        }

        inc = constants.to_json(frm="broker", port=self.addr, typ="sync", data=data)

        for i in self.brokers:
            r = requests.post(f"{constants.LOCALHOST}:{i}", data=inc)

    def thread_send_sync_data(self):
        p = threading.Thread(target=self.send_sync_data)
        p.start()

    def parse_sync_data(self, data):
        """
        Extract data from the sync message sent from broker and update metadata
        :return:
        """
        self.producers = data["producers"]
        self.consumers = data["consumers"]
        self.topics = data["topics"]

    def write_to_partition(self, topic_name, msg):
        """
        Write the message received from publisher to file-system. Writes to the file with the least number of lines
        :return:
        """
        d = {}
        for i in self.topics[topic_name]:
            d[i] = 0

        for i in d:
            with open(i, 'r') as f:
                d[i] = len(f.readlines())

        d = {k: v for k, v in sorted(d.items(), key=lambda item: item[1])}

        with open(list(d.keys())[0], 'a') as f:
            data = {
                "timestamp": datetime.datetime.now().timestamp(),
                "msg": msg
                    }
            f.write(json.dump(data))

    def send_from_beginning(self, topic, port):
        """
        Will read the partition for a given topic and read all the data from it
        :return:
        """
        file_path = self.topics[topic]

        stash = []

        for i in file_path:
            with open(i, 'r') as f:
                stash += f.readlines()

        for i in range(len(stash)):
            stash[i] = stash[i][:-1]    # removing new line character

        for i in range(len(stash)):
            stash[i] = json.loads(stash[i])     # converting the json format to dictionaries

        sorted(stash, key=operator.itemgetter('timestamp')) # sorting the data based on time stamp

        # removing timestamp details from the text
        for i in range(len(stash)):
            stash[i] = stash[i]['msg']

        for i in stash:
            inc = constants.to_json(frm="broker", port=self.addr, typ="publish", topic=topic, data=i)
            r = requests.post(f"{constants.LOCALHOST}:{port}", data=i)

    def push_to_consumer(self, inc):
        new_inc = inc
        new_inc['from'] = "broker"
        new_inc['port'] = self.addr

        new_inc = json.dumps(new_inc)

        for i in self.consumers:
            r = requests.post(f"{constants.LOCALHOST}:{i}", data=new_inc)

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
        inc = constants.to_dict(
            self.rfile.read(int(self.headers['Content-Length'])).decode('utf-8')
        )

        if inc['from'] == "zookeeper":
            if inc["type"] == "set-leader":
                # receives instruction from zookeeper to become the leader
                broker.leader = 1

            if inc["type"] == "sync":
                # received by leader
                # send all relevant metadata to the new broker
                broker.thread_send_sync_data()

        elif inc["from"] == "broker":
            if not broker.leader and inc["type"] == "sync":
                # leader node will transfer all metadata to new broker
                p = threading.Thread(target=broker.parse_sync_data, args=[inc['data']])
                p.start()

        elif inc["from"] == "producer":
            if inc["type"] == "register":
                broker.create_topic(inc["topic"])
                broker.producers.append(inc["port"])
                # sync the metadata
                broker.thread_send_sync_data()

            elif inc["type"] == "publish":
                # collects the message and adds to queue + stores to partition
                # will also push data to other brokers
                if inc['data'] == "pls_die_leader":
                    sys.exit()

                # sending published data to partitions
                p = threading.Thread(target=broker.write_to_partition, args=[inc['topic'], inc['data']])
                p.start()

                # forwarding data to consumers


        elif inc["from"] == "consumer":
            if inc["type"] == "register":
                # added consumer to the pool of consumers and if needed to create topic
                broker.create_topic(inc["topic"])
                broker.consumers.append(inc["port"])
                # sync the metadata
                broker.thread_send_sync_data()

            elif inc["type"] == "from-beginning":
                # give information regarding the files so that the consumer can read all old data
                broker.create_topic(inc["topic"])
                broker.consumers.append(inc["port"])

                # sync the metadata
                broker.thread_send_sync_data()
                p = threading.Thread(target=broker.send_from_beginning, args=[inc["topic"], inc["port"]])
                p.start()


if __name__ == "__main__":
    lead = bool(int(sys.argv[1]))   # leader
    port = int(sys.argv[2])    # index for to select one of the three preset ports

    broker = Broker(constants.ZOOKEEPER_PORT, lead, port)
    broker.start_heartbeat()

    server = HTTPServer(('localhost', port), RequestHandler)
    print(f"Server running on PORT - {server.server_address[0]}:{server.server_address[1]}")
    server.serve_forever()
