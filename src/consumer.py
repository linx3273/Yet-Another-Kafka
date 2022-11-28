import socket
import subprocess

topic=input("Enter the topic: ")

s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

zookeeper_port = 34567

s.connect(('127.0.0.1', zookeeper_port))

s.send(topic.encode())

broker = (s.recv(1024).decode()) # we are currently receiving "port_number, file_name"
broker = broker.split(sep=",") # First element will be the port number, which we have to typecast as int, the second element is name
# print(data)

s.close()


subprocess.run("python "+broker[1]+" server "+topic)

s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

broker_port = int(broker[0])

s.connect(('127.0.0.1', broker_port))

data = (s.recv(1024).decode())

print(data)

s.close()