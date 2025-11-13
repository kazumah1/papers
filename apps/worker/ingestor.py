from optparse import TitledHelpFormatter
import requests
import hashlib
import feedparser
from bs4 import BeautifulSoup
from google.cloud import storage
from PyPDF2 import PdfReader
from jobs import *
import psycopg
import io
import json

class Ingestor:
    def __init__(self):
        self.url:str = "http://export.arxiv.org/api/query?"
        self.entries:[] = []
    
    def _get_url(self, entry):
        '''returns the url of a specific entry'''
        return entry["id"]

    def _get_entries(self, parser_output):
        '''returns the entries array from the raw feedparser output'''
        return parser_output["entries"]


    def search(self, search_queries:list(str), max_results:int=10, page:int=0, sort:str="submittedDate", sort_order:str="descending"):
        search_arg = "+AND+".join(search_queries)
        url = self.url + f'search_query={search_arg}&start={page}&max_results={max_results}&sortBy={sort}&sortOrder={sort_order}'
        response = requests.get(url)
        d = feedparser.parse(response.text)
        entries = self._get_entries(d)
        # for entry in entries:
        #     self.ingest(entry)
        return d

    def read(self, html_url):
        '''
        for returning the raw html/xhtml of the paper html link
        args:
            url: url for the html page
        '''
        # don't like that this takes in url not page
        response = requests.get(html_url)
        content = response.text
        soup = BeautifulSoup(content, 'html.parser')
        print(soup.get_text())
        return soup

    def store(self, serialized_job):
        '''
        for storing the raw pdf of the paper to GCS
        '''
        # ingest doc to object storage
        job = json.dumps(serialized_job)
        pdf_url = job["pdf_url"]
        response = requests.get(pdf_url)
        storage_client = storage.Client()
        bucket = storage_client.bucket('storage-papers')

        filename = job["content_hash"] + ".pdf"
        # whole point of hashing files
        if self._file_exists(filename):
            return
        blob = bucket.blob(f"raw/{filename[:2]}/{filename[2:4]}/{filename}")

        blob.upload_from_string(response.content, content_type="application/pdf")

    def db_push(self, serialized_job):
        
        job = json.dumps(serialized_job)
