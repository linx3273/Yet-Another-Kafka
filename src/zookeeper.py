import socket
import subprocess


if __name__ == "__main__":
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    except:
        print("Error in creating socket")
        exit(1)

    # host = socket.gethostname()
    port = 34567

    s.bind(('127.0.0.1', port))

    s.listen()

    while True:
        c, addr = s.accept()
        topic = c.recv(1024).decode()
        # subprocess.call('start /wait python producer.py {} {}'.format(addr, topic), shell=True)
        data = str(23456)+", broker.py" # Here we need to pass the port number and name of the broker with which we are connecting
        c.send(data.encode())
        c.close()