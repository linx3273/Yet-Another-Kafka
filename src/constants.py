ZOOKEEPER_PORT = 8000
BROKER_PORT = [8001, 8002, 8003]
LEADER = 1
NOT_LEADER = 0
TIME_LIMIT = 15
INTERVALS = 5

'''
Functionality communications to be enclosed within // // 

eg Leader broker will send a pulse by enclosing it's port within // //
i.e. for port 8001 the POST message will be  //8001//
'''

'''
DAEMON
False - Process will continue even after parent dies
True - Process will end after parent dies 
'''