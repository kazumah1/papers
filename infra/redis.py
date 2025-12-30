from dotenv import load_dotenv
import redis
import os

def _redis_server():
    load_dotenv()
    if os.getenv("DEVELOPMENT") == 'true':
        url = os.getenv("REDIS_HOST_DEV")
        port = os.getenv("REDIS_PORT")
        pw = ""
    else:
        url = os.getenv("REDIS_HOST")
        port = os.getenv("REDIS_PORT")
        pw = os.getenv("REDIS_PASSWORD")
    r = redis.Redis(
        host=url,
        port=port,
        decode_responses=True,
        username="default",
        password=pw
    )
    return r

