from optparse import TitledHelpFormatter
import requests
import hashlib
import feedparser
from bs4 import BeautifulSoup
from PyPDF2 import PdfReader
from infra.postgres import _postgres_db, new_conn
from infra.gcs import upload_paper
from utils.utils import Colors
import psycopg
import io
import json

URL : str = "http://export.arxiv.org/api/query?"

def _get_url(entry):
    '''returns the url of a specific entry'''
    return entry["id"]

def _get_entries(parser_output):
    '''returns the entries array from the raw feedparser output'''
    return parser_output["entries"]

def search(search_queries:list(str), max_results:int=10, page:int=0, sort:str="submittedDate", sort_order:str="descending"):
    global URL
    search_arg = "+AND+".join(search_queries)
    url = URL + f'search_query={search_arg}&start={page}&max_results={max_results}&sortBy={sort}&sortOrder={sort_order}'
    response = requests.get(url)
    d = feedparser.parse(response.text)
    entries = _get_entries(d)
    # for entry in entries:
    #     ingest(entry)
    return entries

def store(serialized_job):
    '''
    for storing the raw pdf of the paper to GCS
    '''
    # ingest doc to object storage
    job = json.loads(serialized_job)
    pdf_url = job["pdf_url"]
    response = requests.get(pdf_url)
    filename = job["content_hash"] + ".pdf"

    upload_paper(filename, response.content)

    print(f"{Colors.GREEN}Successfully stored paper to GCS{Colors.WHITE}")

def db_push(serialized_job):
    job = json.loads(serialized_job)

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
