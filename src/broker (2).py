import socket
import sys


def client(topic):
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

    producer_parent_port = 12345

    s.connect(('127.0.0.1', producer_parent_port))


    s.send(topic.encode())

    data = (s.recv(1024).decode())

    print(data)
    s.close()
    return data


def server(topic):
    data = client(topic)
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    port = 23456
    s.bind(('127.0.0.1', port))
    s.listen()

    while True:
        c, addr = s.accept()
        # topic = c.recv(1024).decode()
        c.send(data.encode())
        c.close()
        break
    s.close()
    return

if __name__ == "__main__":
    if sys.argv[1] == "client":
        client(sys.argv[2])
    else:
        server(sys.argv[2])