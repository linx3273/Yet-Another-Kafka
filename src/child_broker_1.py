import socket
import sys
import json
import os
import threading
import time

def send_heartbeat():
    zookeeper_heartbeat_port = 56789

    while True:
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        time.sleep(1)
        s.connect(('127.0.0.1', zookeeper_heartbeat_port))
        s.send("Heartbeat from Broker 2\n".encode())
        s.close()


def get_data_from_file(topic):
    # data = {"topic": "my_topic", "data": "my_data"}
    # data = json.dumps(data)
    filename = 'leader.json'
    with open(filename, "r") as file:
        data = json.load(file)
    return data[topic]


# def server(topic):
#     data = client(topic)
#     s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
#     port = 23456
#     s.bind(('127.0.0.1', port))
#     s.listen()
#
#     while True:
#         c, addr = s.accept()
#         # topic = c.recv(1024).decode()
#         c.send(data.encode())
#         c.close()
#         break
#     s.close()
#     return

def receive_the_data():
    # Receiving the new data
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    producer_parent_secondary_port = 54321
    s.connect(('127.0.0.1', producer_parent_secondary_port))
    new_data = s.recv(1024).decode()
    s.close()
    new_data=json.loads(new_data)
    filename = 'leader.json'
    if os.path.exists(filename):
        with open(filename, "r") as file:
            data = json.load(file)
        # data.append(new_data)
        data[new_data["topic"]] = new_data["data"]
    else:
        data = {}
        data[new_data["topic"]] = new_data["data"]
    with open(filename, "w") as file:
        json.dump(data, file)


if __name__ == "__main__":
    heartbeat_thread = threading.Thread(target=send_heartbeat)
    heartbeat_thread.start()
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    port = 17894
    s.bind(('127.0.0.1', port))
    s.listen()
    while True:
        # if sys.argv[1] == "client":
        #     client(sys.argv[2])
        # else:
        #     server(sys.argv[2])
        c, addr = s.accept()
        status_or_topic = c.recv(1024).decode()
        if status_or_topic == "new_topic":
            receive_the_data()
        else:
            data = get_data_from_file(status_or_topic)
            c.send(data.encode())
            c.close()