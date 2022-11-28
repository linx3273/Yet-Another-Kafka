import socket
import subprocess


subprocess.run("python broker.py server my_topic")


s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

leader_port = 23456

s.connect(('127.0.0.1', leader_port))

data = (s.recv(1024).decode())

print(data)

s.close()