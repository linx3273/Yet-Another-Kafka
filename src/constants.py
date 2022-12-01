import json


ZOOKEEPER_PORT = 8000
BROKER_PORT = [8001, 8002, 8003]
LEADER = 1
NOT_LEADER = 0
TIME_LIMIT = 15
INTERVALS = 5
LOCALHOST = "http://127.0.0.1"


def to_json(frm=None, port=None, typ=None, topic=None, data=None):
    return json.dumps(
        {
            "from": frm,        # zookeeper broker producer consumer
            "port": port,
            "type": typ,        # set-leader, publish, register, from-beginning, sync
            "topic": topic,     # default is None
            "data": data,       # default is Blank
        },
        indent=4
    )


def to_dict(val):
    return json.loads(val)


'''
DAEMON
False - Process will continue even after parent dies
True - Process will end after parent dies 
'''
