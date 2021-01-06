import redis

pool = None

class Redis:

    def __init__(self):
        self.connection = None
        self.connect()

    def connect(self):
        global pool
        try:
            pool = redis.Redis(
                host='192.168.1.106',
                port=6379,
                password=None,
                db=0
            )
            self.connection = redis.Redis(connection_pool=pool)
        except Exception as e:
            raise Exception("Failed to connect to Redis: {0}".format(e.message))

    def set_param(self):
        pass