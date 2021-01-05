import redis

class Redis():

    def __init__(self):
        self.connection = None
    
    def connect(self):
        try:
            self.connection = redis.Redis(
                host='192.168.1.106',
                port=6379,
                password=None,
                db=0
            )
        except Exception as e:
            raise Exception("Failed to connect to Redis: {0}".format(e.message))

    def set_param(self):
        pass