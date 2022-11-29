import pika
from pika.exchange_type import ExchangeType
import time
import random

def msg_rec(channel,method,prop,body):
    # simulating bg task
    # t = random.randint(1,6)
    # print(f"received message in 1 is : {body}, will take {t} to process")
    # time.sleep(t)
    # channel.basic_ack(delivery_tag=method.delivery_tag) 
    print(f"SUBSCRIBER3 says hello with message : {body}") 
    print("Finished msg processing")  

connection_param = pika.ConnectionParameters('localhost')
connection = pika.BlockingConnection(connection_param)

#creating multiple channels
channel = connection.channel()

#creating a queue to store the data
channel.exchange_declare(exchange='model_topics',exchange_type=ExchangeType.topic)

q = channel.queue_declare(queue='',exclusive=True)
# setting the quality of service with the prefetch value as 1
# each consumer can process only one msg at a time
# for fair dispatch mechanism
# channel.basic_qos(prefetch_count=1)
# if we dont use it then by default round robin algo is impelemented

# binding our queue to the desired channel
# we can assign multiple routing keys to each of the subscriber

channel.queue_bind(exchange='model_topics',queue=q.method.queue,routing_key="#.3")

# manually acknowledges the message and defining the functionality it does when it receives a new message
channel.basic_consume(queue=q.method.queue,auto_ack=True,on_message_callback=msg_rec)

print("Started consuming the msg")

channel.start_consuming()


