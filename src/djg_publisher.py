import pika
from pika.exchange_type import ExchangeType
import random
import time

connection_param = pika.ConnectionParameters('localhost')
connection = pika.BlockingConnection(connection_param)

#creating multiple channels
channel = connection.channel()

channel.exchange_declare(exchange='model_topics',exchange_type=ExchangeType.topic)
#creating a queue to store the data
# producer is not responsible for declaring a queue bcz consumers have their own dedicated queues and publisher has no idea about it
# channel.queue_declare(queue="broker")

msg1 = "message for only SUBSCRIBER 1"
msg2 = "message for only SUBSCRIBER 2"
msg3 = "message for only SUBSCRIBER 3"
msg = "msg to everyone from check@test.mail"
channel.basic_publish(exchange='model_topics', routing_key='sub.1', body=msg1)
channel.basic_publish(exchange='model_topics', routing_key='sub.2', body=msg2)
channel.basic_publish(exchange='model_topics', routing_key='sub.3', body=msg3)
channel.basic_publish(exchange='model_topics', routing_key='3', body=msg)

# CAN IMPLEMENT MULTIPLE MESSAGING LATER
# i = 1
# # settin infinite loop to keep publishing message
# while(True):
#     msg1 = f"This is the {i}th message to my broker"
#     channel.basic_publish(exchange='', routing_key='broker', body=msg1)
#     print(f"message sent is : {msg1}")
#     time.sleep(random.randint(1,4))
#     i+=1

print(f"message sent is : {msg1}")
print(f"message sent is : {msg2}")
print(f"message sent is : {msg3}")
print(f"message sent is : {msg}")
connection.close()
