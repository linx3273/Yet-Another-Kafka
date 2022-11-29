import socket
import subprocess
import threading


def heartbeat_check():
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    zookeeper_heartbeat = 56789
    s.bind(('127.0.0.1', zookeeper_heartbeat))
    s.listen()
    while True:
        c, addr = s.accept()
        heartbeat = c.recv(1024).decode()
        print(heartbeat)
        c.close()


if __name__ == "__main__":
    heartbeat = threading.Thread(target=heartbeat_check)
    heartbeat.start()
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
        data = str(23456) # Here we need to pass the port number of the broker with which we are connecting
        c.send(data.encode())
        c.close()