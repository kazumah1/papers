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

r = _redis_server()

def cache_pdf(paper_id: str, pdf_bytes:bytes, ttl_sec: int=3600):
    r.set(f"pdf:{paper_id}", pdf_bytes, ex=ttl_sec)

def get_cached_pdf(paper_id:str) -> bytes | None:
    return r.get(f"pdf:{paper_id}")