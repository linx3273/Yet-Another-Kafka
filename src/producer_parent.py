import socket
import subprocess


def receive_the_data_from_publisher():
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

    producer_port = 45678

    s.connect(('127.0.0.1', producer_port))

    data = (s.recv(1024).decode())

    # print(data)
    s.close()
    return data

def send_to_broker(data):
    # Getting leader port from zookeeper
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

    zookeeper_port = 34567

    s.connect(('127.0.0.1', zookeeper_port))

    s.send("my_topic".encode()) # Send the topic name here

    broker_port = int(s.recv(1024).decode())

    s.close()

    #Connecting to broker and telling about data
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

    s.connect(('127.0.0.1', broker_port))

    s.send("new_topic".encode())

    s.close()

    # Sending the data
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.bind(('127.0.0.1', 54321))
    s.listen()

    while True:
        c, addr = s.accept()
        c.send(data.encode())
        c.close()
        break
    return

if __name__ == "__main__":
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    except:
        print("Error in creating socket")
        exit(1)

    # host = socket.gethostname()
    port = 12345

    s.bind(('127.0.0.1', port))

    s.listen()

    while True:
        c, addr = s.accept()
        role = c.recv(1024).decode()
        # subprocess.call('start /wait python producer.py {} {}'.format(addr, topic), shell=True)
        if role == "publishing":
            data = receive_the_data_from_publisher()
            send_to_broker(data)
        # data = subprocess.check_output("python producer.py {}".format(topic))
        #     c.send(data.encode())
            c.close()