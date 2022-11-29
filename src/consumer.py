import socket
import subprocess

topic = input("Enter the topic: ")

s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

zookeeper_port = 34567

s.connect(('127.0.0.1', zookeeper_port))

s.send(topic.encode())

broker = (s.recv(1024).decode())# we are currently receiving leader "port_number" here

s.close()


# subprocess.run("python "+broker[1]+" server "+topic)

s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

broker_port = int(broker)

s.connect(('127.0.0.1', broker_port))

s.send(topic.encode())

data = (s.recv(1024).decode())

print(data)

s.close()