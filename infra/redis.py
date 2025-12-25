from dotenv import load_dotenv
import redis
import os

def _redis_server():
    load_dotenv()
    if os.getenv("DEVELOPMENT") == 'true':
        url = os.getenv("REDIS_URL_DEV")
        pw = ""
    else:
        url = os.getenv("REDIS_URL")
        pw = os.getenv("REDIS_PASSWORD")
    r = redis.Redis(
        host=url,
        port=11283,
        decode_responses=True,
        username="default",
        password=pw
    )
    return r

