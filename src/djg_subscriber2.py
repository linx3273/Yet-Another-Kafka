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
    print(f"sub2 says hello with message : {body}") 
    print("Finished msg processing")  

connection_param = pika.ConnectionParameters('localhost')
connection = pika.BlockingConnection(connection_param)

#creating multiple channels
channel = connection.channel()

#creating a queue to store the data
channel.exchange_declare(exchange='ps_model1',exchange_type=ExchangeType.direct)

q = channel.queue_declare(queue='',exclusive=True)
# setting the quality of service with the prefetch value as 1
# each consumer can process only one msg at a time
# for fair dispatch mechanism
# channel.basic_qos(prefetch_count=1)
# if we dont use it then by default round robin algo is impelemented

# binding our queue to the desired channel
channel.queue_bind(exchange='ps_model1',queue=q.method.queue,routing_key="sub2")
# manually acknowledges the message and defining the functionality it does when it receives a new message
channel.basic_consume(queue=q.method.queue,auto_ack=True,on_message_callback=msg_rec)

print("Started consuming the msg")
channel.start_consuming()
# messages from queue are sent to consumers in round-robin way by default


