import redis
from dotenv import load_dotenv
import os
import json
import psycopg
import hashlib
import requests
from PyPDF2 import PdfReader
import io
from rq import Queue
from ingestor import Ingestor
from processor import Processor

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

def get_job():
    r = _redis_server()
    job = r.rpop("jobs")
    unserialized_job = json.loads(job)
    return unserialized_job

def _postgres_db():
    load_dotenv()
    if os.getenv("DEVELOPMENT") == "true":
        db_name = os.getenv("POSTGRES_DB_DEV")
        db_user = os.getenv("POSTGRES_USER_DEV")
        db_pass = os.getenv("POSTGRES_PASSWORD_DEV")
    else:
        db_name = os.getenv("POSTGRES_DB_PROD")
        db_user = os.getenv("POSTGRES_USER_PROD")
        db_pass = os.getenv("POSTGRES_PASSWORD_PROD")
    conn = psycopg.connect(dbname=db_name, user=db_user, password=db_pass, host="localhost", port=5433)
    conn.execute("""
        SELECT EXISTS (
            SELECT FROM information_schema.tables
            WHERE table_name = 'papers'
        )
    """)
    try:
        table_exists = conn.cursor().fetchone()
    except Exception:
        table_exists = None
    if not table_exists:
        conn.execute("""
            CREATE EXTENSION IF NOT EXISTS 'pgcrypto'
        """)
        conn.execute("""
            CREATE TABLE Papers (
                id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                external_id STRING NOT NULL UNIQUE,
                title STRING NOT NULL,
                authors STRING[] NOT NULL,
                pdf_url STRING NOT NULL,
                html_url STRING NOT NULL,
                content_hash BINARY(32) NOT NULL UNIQUE,
                published_at DATETIME NOT NULL
            )
        """)
        conn.commit()
    else:
        conn.execute("DROP TABLE Papers")
        conn.commit()
    return conn

def db_get(table, id):
    conn = _postgres_db()
    conn.execute(f"""
        SELECT * FROM {table} WHERE id == {id}
    """)
    for record in conn:
        print(record)
    for record in conn:
        return record

def db_add(metadata):
    # TODO: verify metadata is in right format
    conn = _postgres_db()
    # TODO: ensure metadata has this format: (id, title, author, pdf_url, html_url, current_datetime)
    conn.execute(
        f"INSERT INTO Papers (id, title, author, pdf_url, html_url, created_at) VALUES (%(id)s, %(title)s, %(author)s, %(pdf_url)s, %(html_url)s, %(created_at)s)",
        metadata
    )
    return True

class ArxivDataManager:
    def __init__(self):
        ...
    def convert_url_to_html_url(self, url):
        '''converts original arxiv paper url to html url'''
        html_url = url[:17] + 'html' + url[20:] 
        return html_url
    
    def get_pdf_url(self, entry):
        links = entry["links"]
        for link in links:
            if "title" in link:
                if link["title"] == 'pdf':
                    return link["href"]
    
    def get_authors(self, entry):
        return [a['name'] for a in entry['authors']]

    def get_tags(self, entry):
        tags = []
        primaries = set()
        for tag in entry['tags']:
            tags.append(tag['term'])
            if "." in tag['term']:
                primaries.add(tag['term'].split('.')[0])
        return tags + list(primaries)


class JobManager:
    def __init__(self):
        self.redis = None
        self.ingest_q = None
        self.process_q = None

        # workers
        self.ingestor = Ingestor()
        self.processor = Processor()

        # data managers for creating jobs
        self.arxiv = ArxivDataManager()

        # job types as of now
        self.JOBS = {
            'store': self.ingestor.store, 
            'embed': self.processor.embed, 
            'db_push': self.ingestor.db_push, 
            'figures': self.processor.figures, 
            'summarize': self.processor.summarize, 
            'keywords': self.processor.keywords
        }


        self.initialize_redis()
 
    def initialize_redis(self):
        load_dotenv()
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
        self.redis = r
        self.ingest_q = Queue("ingest", connection=self.redis)
        self.process_q = Queue("process", connection=self.redis)

    def hash_file(self, pdf_url):
        response = requests.get(pdf_url)
        mem_object = io.BytesIO(response.content)
        file = PdfReader(mem_object)
        h = hashlib.sha256()
        for page in file.pages:
            text = page.extract_text()
            text_bytes = text.encode('utf-8')
            h.update(text_bytes)
        return h.hexdigest()
    
    def add_job(self, job: dict):
        r = self.redis
        required_fields = set(
            "id",
            "title",
            "authors",
            "pdf_url",
            "html_url",
            "source",
            "content_hash",
            "license",
            "published_at",
            "tags",
            "job_type"
        )
        for key in job.keys():
            if key not in required_fields:
                raise ValueError("missing or incorrect key: ", key)
        serialized_job = json.loads(job)
        if job['job_type'] in ['store', 'db_push']:
            self.ingest_q.enqueue(self.JOBS[job['job_type']], serialized_job)
        else:
            self.process_q.enqueue(self.Jobs[job['job_type']], serialized_job)
        

    def create_job_set(self, entry):
        '''
        for creating all subjobs once an entry is received
        types:
            - store: store raw pdf into gcs (ingestor)
            - embed: store text embeddings to vector db (pgvector) for semantic search (processor)
            - db_push: insert new postgres row (ingestor)
            - figures: extract figures from pdf/html, store in gcs, map to corresponding pdf (processor)
            - summarize: use llm to summarize paper to display on frontend (processor)
            - keywords: use tsvector to extract keywords from paper for later search and tagging (processor)
        '''
        pdf_url = self.arxiv.get_pdf_url(entry)
        content_hash = self.hash_file(pdf_url)
        job_id = "arxiv:" + entry['id'][21:]
        authors = self.arxiv.get_authors(entry)
        html_url = self.arxiv.convert_url_to_html_url(entry["id"])
        title = entry['title']
        publish_date = entry['published']
        tags = self.arxiv.get_tags(entry)
        for job_type in self.JOBS.keys():
            job = {
                "id":job_id,
                "title":title,
                "authors":authors,
                "pdf_url":pdf_url,
                "html_url":html_url,
                "source":"arxiv",
                "content_hash":content_hash,
                "license":"",
                "published_at":publish_date,
                "tags":tags,
                "job_type":job_type
            }
            self.add_job(job)