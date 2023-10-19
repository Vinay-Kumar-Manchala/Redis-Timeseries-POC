import os
import redis
import psycopg2
from dotenv import load_dotenv
from contextlib import contextmanager
from psycopg2.extras import RealDictCursor
load_dotenv()


class SqlDbReader:
    def __init__(self):
        self.connection = psycopg2.connect(
            host=os.environ.get('POSTGRES_HOST'),
            user=os.environ.get('POSTGRES_USER'),
            password=os.environ.get('POSTGRES_PASSWORD'),
            dbname=os.environ.get('POSTGRES_DB'),
            port=os.environ.get('POSTGRES_PORT')
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
        self.redis_client = redis.StrictRedis(host=os.environ.get('REDIS_HOST'),
                                              port=int(os.environ.get('REDIS_PORT')),
                                              decode_responses=True)

    @contextmanager
    def redis_connect(self):
        try:
            yield self.redis_client

        finally:
            self.redis_client.close()