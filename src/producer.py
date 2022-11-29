import sys
import socket
import json

# if sys.argv[1] == "my_topic":
#     print("This is: ", sys.argv[1])
#
# elif sys.argv[1] == "second_try":
#     print("This is: ", sys.argv[1])
#
# else:
#     print("Not a valid topic")

def send_the_data(topic_name, text_data):
    data = {"topic": topic_name, "data":text_data}
    data = json.dumps(data)
    # print(data)
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    port = 45678
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
    topic_name = input("Enter the name of the topic: ")
    text_data = input("Enter the data associated with the topic: ")

    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

    producer_parent_port = 12345

    s.connect(('127.0.0.1', producer_parent_port))

    s.send("publishing".encode())

    send_the_data(topic_name, text_data)

    s.close()


