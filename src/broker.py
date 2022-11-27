import os
from pathlib import Path
import sys
from http.server import HTTPServer, BaseHTTPRequestHandler


BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(sys.argv[0]), os.pardir))


class Broker(BaseHTTPRequestHandler):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.topics = {}    # holds topic name and file pointers

        self.brokers = []
        self.producers = []
        self.consumers = []

        self.topic_dir = Path(BASE_DIR + '\\topics').resolve().as_posix()
        self.src_dir = Path(BASE_DIR + '\\src').resolve().as_posix()
        self.log_dir = Path(BASE_DIR + '\\logs').resolve().as_posix()

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

    def set_response(self):
        self.send_response(200)
        self.send_header("Content-type", "application/json")
        self.end_headers()

    def do_GET(self):
        # TODO
        self.send_response()
        self.wfile.write()

    def do_POST(self):
        # TODO
        self.set_response()
        print(self.rfile.read(int(self.headers['Content-Length'])).decode('utf-8'))


if __name__ == "__main__":
    server = HTTPServer(('localhost', 8000), Broker)
    print(f"Server running on {server.server_address[0]}:{server.server_address[1]}")
    server.serve_forever()
