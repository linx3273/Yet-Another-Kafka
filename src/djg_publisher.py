import pika
from pika.exchange_type import ExchangeType
import random
import time

connection_param = pika.ConnectionParameters('localhost')
connection = pika.BlockingConnection(connection_param)

#creating multiple channels
channel = connection.channel()

channel.exchange_declare(exchange='ps_model1',exchange_type=ExchangeType.direct)
#creating a queue to store the data
# producer is not responsible for declaring a queue bcz consumers have their own dedicated queues and publisher has no idea about it
# channel.queue_declare(queue="broker")

msg1 = f"This is the message using my routing"
channel.basic_publish(exchange='ps_model1', routing_key='sub1', body=msg1)

# CAN IMPLEMENT MULTIPLE MESSAGING LATER
# i = 1
# # settin infinite loop to keep publishing message
# while(True):
#     msg1 = f"This is the {i}th message to my broker"
#     channel.basic_publish(exchange='', routing_key='broker', body=msg1)
#     print(f"message sent is : {msg1}")
#     time.sleep(random.randint(1,4))
#     i+=1
