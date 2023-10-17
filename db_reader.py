import redis
import psycopg2
from contextlib import contextmanager
from psycopg2.extras import RealDictCursor


class SqlDbReader:
    def __init__(self):
        self.connection = psycopg2.connect(
            host="34.100.243.227",
            user="root",
            password="Secret@123",
            dbname="postgres",
            port="5432"
        )
        self.cursor_type = RealDictCursor

    @contextmanager
    def sql_connect(self):
        cursor = self.connection.cursor(cursor_factory=self.cursor_type)
        try:
            yield cursor

        finally:
            cursor.close()

    @contextmanager
    def dbconnect(self):    
        try:
            yield self.connection
        finally:
            self.connection.close()


class RedisDbReader:
    def __init__(self):
        self.redis_client = redis.StrictRedis(host="192.168.0.210", port=4004, decode_responses=True)

    @contextmanager
    def redis_connect(self):
        try:
            yield self.redis_client

        finally:
            self.redis_client.close()