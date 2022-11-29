import pika
import random
import time

connection_param = pika.ConnectionParameters('localhost')
connection = pika.BlockingConnection(connection_param)

#creating multiple channels
channel = connection.channel()

#creating a queue to store the data
channel.queue_declare(queue="broker")

i = 1
# settin infinite loop to keep publishing message
while(True):
    msg1 = f"This is the {i}th message to my broker"
    channel.basic_publish(exchange='', routing_key='broker', body=msg1)
    print(f"message sent is : {msg1}")
    time.sleep(random.randint(1,4))
    i+=1
