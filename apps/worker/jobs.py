import redis
from dotenv import load_dotenv
import os
import json
import psycopg
import hashlib
import requests
from PyPDF2 import PdfReader
import io
from rq import Queue, Worker
from infra.postgres import _postgres_db, _vector_db, _images_db, new_conn, db_search_by_pdf_url
from infra.redis import get_cached_pdf, cache_pdf
from apps.worker.processor import *
from utils.utils import Colors
from pgvector.psycopg import register_vector

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
        self.worker = None

        # data managers for creating jobs
        self.arxiv = ArxivDataManager()

        # job types as of now
        self.JOBS = {
            'embed': embed, 
            'figures': figures, 
            'summarize': summarize, 
            'keywords': keywords
        }

        print("Loaded papers table:", _postgres_db())
        print("Loaded vectors table:", _vector_db())
        print("Loaded images table:", _images_db())

        self.initialize_redis()

    def store(self, job: dict):
        '''
        for storing the raw pdf of the paper to GCS
        '''
        # ingest doc to object storage
        pdf_content = get_cached_pdf(job['external_id'])
        if pdf_content is None:
            pdf_url = job["pdf_url"]
            response = requests.get(pdf_url)
            pdf_content = response.content
            cache_pdf(job['external_id'], pdf_content)
        filename = job["content_hash"] + ".pdf"

        upload_paper(filename, pdf_content)

        print(f"{Colors.GREEN}Successfully stored paper to GCS{Colors.WHITE}")

    def db_push(self, job: dict):
        with new_conn() as conn:
            conn.execute(
                f"""INSERT INTO Papers 
                    (external_id, source, title, authors, pdf_url, html_url, content_hash, published_at) 
                    VALUES 
                    (%(id)s, %(source)s, %(title)s, %(authors)s, %(pdf_url)s, %(html_url)s, %(content_hash)s, %(published_at)s)
                    ON CONFLICT (external_id) DO NOTHING;
                """,
                job
            )
            conn.commit()
        print(f"{Colors.GREEN}Successfully stored initial DB entry{Colors.WHITE}")

 
    def initialize_redis(self):
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
            decode_responses=False,
            username="default",
            password=pw
        )
        self.redis = r
        # self.ingest_q = Queue("ingest", connection=self.redis)
        # self.process_q = Queue("process", connection=self.redis)
        # self.worker = Worker([self.ingest_q, self.process_q], connection=self.redis)

    def hash_file(self, pdf_url):
        pdf_content = get_cached_pdf(job['external_id'])
        if pdf_content is None:
            pdf_url = job["pdf_url"]
            response = requests.get(pdf_url)
            pdf_content = response.content
            cache_pdf(job['external_id'], pdf_content)
        mem_object = io.BytesIO(pdf_content)
        file = PdfReader(mem_object)
        h = hashlib.sha256()
        for page in file.pages:
            text = page.extract_text()
            text_bytes = text.encode('utf-8')
            h.update(text_bytes)
        return h.hexdigest()
    
    def add_job(self, job: dict):
        r = self.redis
        # TODO: use pydantic
        required_fields = {
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
            }
        for field in required_fields:
            if field not in job.keys():
                raise ValueError("missing or incorrect field: ", field)
        serialized_job = json.dumps(job)
        res = r.xadd("job_queue", {"job" : serialized_job}, maxlen=5000, approximate=False)
        # if job['job_type'] in {'store', 'db_push'}:
        #     self.ingest_q.enqueue(self.JOBS[job['job_type']], serialized_job)
        # else:
        #     self.process_q.enqueue(self.JOBS[job['job_type']], serialized_job)
        

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
        if pdf_url is None:
            pdf_url = ""
        else:
            records = db_search_by_pdf_url(pdf_url)
            if records:
                return
        content_hash = self.hash_file(pdf_url)
        job_id = "arxiv." + entry['id'][21:]
        authors = self.arxiv.get_authors(entry)
        html_url = self.arxiv.convert_url_to_html_url(entry["id"])
        title = entry['title']
        publish_date = entry['published']
        tags = self.arxiv.get_tags(entry)
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
            "tags":tags
        }
        self.store(job)
        self.db_push(job)
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
            self.add_job(job=job)


    def start_workers(self):
        retries = 3
        while True:
            try:
                jobs = self.redis.xread(streams={"job_queue":0}, count=6, block=300)
                if jobs and jobs[0] and jobs[0][1]:
                    j = 0
                    while j < 6:
                        job = jobs[0][1][j]
                        job_id, serialized_job = job[0], job[1].get(b'job', None)
                        serialized_job = serialized_job.decode("utf-8")
                        unserialized_job = json.loads(serialized_job)
                        job_func = self.JOBS[unserialized_job['job_type']]
                        print(f"{Colors.BLUE}Job: {unserialized_job['job_type']}{Colors.WHITE}")
                        print(job[1])
                        try:
                            job_func(serialized_job)
                        except Exception as e:
                            print(f"{Colors.RED}{unserialized_job['job_type']} failed with exception {e}{Colors.WHITE}")
                            i = 1
                            success = False
                            while i <= retries and success == False:
                                print(f"Retrying, attempt {i} / {retries}:")
                                try:
                                    job_func(serialized_job)
                                    success = True
                                except Exception as e:
                                    print(f"{Colors.RED}attempt failed{Colors.WHITE}")
                                i += 1
                        self.redis.xdel("job_queue", job_id)
                        j += 1
            except (KeyboardInterrupt, SystemExit):
                self.jobs_info()
                raise
            

    def jobs_info(self): 
        print("Queue State")
        print(f"queue length: {self.redis.xlen('job_queue')}")
        # print("Ingest Workers")
        # for w in ingest_workers:
        #     print(f"Worker {w.name}:")
        #     print("Successful Jobs | Failed Jobs | Total Working Time")
        #     print(f"   {w.successful_job_count}   |   {w.failed_job_count}   |   {w.total_working_time}   ")

        # print()
        # print("Process Workers")
        # for w in process_workers:
        #     print(f"Worker {w.name}:")
        #     print("Successful Jobs | Failed Jobs | Total Working Time")
        #     print(f"   {w.successful_job_count}   |   {w.failed_job_count}   |   {w.total_working_time}   ")

    def clear_job_queue(self):
        res = self.redis.xtrim("job_queue", maxlen=0, approximate=False)
        print(res)
        self.jobs_info()

if __name__ == "__main__":
    job_manager = JobManager()
    job_manager.clear_job_queue()
    entries = search(search_queries=["quantum physics"])
    entry = entries[0]
    # print(entry)
    job_manager.create_job_set(entry)
    job_manager.start_workers()
