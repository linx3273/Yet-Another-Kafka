import socket
import subprocess

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
    topic = c.recv(1024).decode()
    # subprocess.call('start /wait python producer.py {} {}'.format(addr, topic), shell=True)
    data = subprocess.check_output("python producer.py {}".format(topic))
    c.send(data.encode())
    c.close()