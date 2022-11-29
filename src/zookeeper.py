from http.server import HTTPServer, BaseHTTPRequestHandler
import constants
import subprocess


class Zookeeper(BaseHTTPRequestHandler):
    def __init__(self, *args, **kwargs):
        self.counter = constants.COUNTER

        super.__init__(*args, **kwargs)
        # TODO

    def create_brokers(self):
        """
        Will start three Brokers
        :return:
        """
        pass

    def monitor_leader(self):
        """
        Method to listen monitor the life of the lead Broker
        :return:
        """
        # TODO
        pass

    def handle_broker_failure(self):
        """
        Incase the lead Broker dies, this method will pick another Broker as the leader
        :return:
        """
        # TODO
        pass

    def do_POST(self):
        """
        Handles post requests sent to the Zookeeper
        :return:
        """
        # TODO
        pass

    def parse_message(self):
        """
        Obtains the message enclosed within // //
        :return:
        """
        # TODO
        pass

    def reset_counter(self):
        """
        Resets the counter back to the preset value
        :return:
        """
        self.counter = constants.COUNTER


if __name__ == "__main__":
    server = HTTPServer(('localhost', constants.ZOOKEEPER_PORT), Zookeeper)
    server.serve_forever()
