import redis

class SistemaClasificacion:
    def __init__(self):
        self.r = redis.Redis("localhost", port=6379, decode_responses=True)
        self.r.flushdb()
        