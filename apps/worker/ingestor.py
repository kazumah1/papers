import requests
import hashlib
import feedparser
from bs4 import BeautifulSoup
from google.cloud import storage
from PyPDF2 import PdfReader
import io

class Ingestor:
    '''Base Ingestor Class - probably not going to be upkept because im lazy'''
    def __init__(self):
        ...
    def hash_file(self):
        ...
    def search(self, search_queries:list(str), max_results:int=10, page:int=0, sort:str="submittedDate", sort_order:str="descending"):
        d = feedparser.parser("")
        return d
    def read(self, url):
        soup = BeautifulSoup('', 'html.parser')
        return soup
    def ingest(self):
        ...
    def create_job(self):
        ...

class ArXivIngestor(Ingestor):
    def __init__(self):
        super().__init__()
        self.url:str = "http://export.arxiv.org/api/query?"
        self.html_url:str|None = None
        self.pdf_url:str|None = None
        self.entries:[] = []
    def _get_url(self, entry):
        '''returns the url of a specific entry'''
        return entry["id"]

    def _set_entries(self, parser_output):
        '''returns the entries array from the raw feedparser output'''
        return parser_output["entries"]

    def _convert_url_to_html_url(self, url):
        '''converts original arxiv paper url to html url'''
        html_url = url[:17] + 'html' + url[20:] 
        return html_url
    
    def _get_pdf_url(self, entry):
        links = entry["links"]
        for link in links:
            if link["title"] == 'pdf':
                return link["href"]

    def _file_exists(self, filename):
        storage_client = storage.Client()
        bucket = storage_client.bucket('storage-papers')

        return storage.Blob(bucket=bucket, name=f"{filename[:2]}/{filename[2:4]}/{filename}").exists(storage_client)

    def hash_file(self):
        response = requests.get(self.pdf_url)
        mem_object = io.BytesIO(response.content)
        file = PdfReader(mem_object)
        h = hashlib.sha256()
        for page in file.pages:
            text = page.extract_text()
            text_bytes = text.encode('utf-8')
            h.update(text_bytes)
        return h.hexdigest()


    def search(self, search_queries:list(str), max_results:int=10, page:int=0, sort:str="submittedDate", sort_order:str="descending"):
        search_arg = "+AND+".join(search_queries)
        url = self.url + f'search_query={search_arg}&start={page}&max_results={max_results}&sortBy={sort}&sortOrder={sort_order}'
        response = requests.get(url)
        d = feedparser.parse(response.text)
        self.entries = self._set_entries(d)
        return d

    def read(self):
        '''
        for returning the raw html/xhtml of the paper html link
        args:
            url: url for the html page
        '''
        response = requests.get(self.html_url)
        content = response.text
        soup = BeautifulSoup(content, 'html.parser')
        print(soup.get_text())
        return soup

    def ingest(self):
        '''
        for storing the raw pdf of the paper to GCS
        '''
        # ingest doc to object storage
        response = requests.get(self.pdf_url)
        storage_client = storage.Client()
        bucket = storage_client.bucket('storage-papers')

        filename = self.hash_file() + ".pdf"
        # whole point of hashing files
        if self._file_exists(filename):
            return
        blob = bucket.blob(f"raw/{filename[:2]}/{filename[2:4]}/{filename}")

        blob.upload_from_string(response.content, content_type="application/pdf")

        self.create_job()

        
    def create_job(self):
        '''
        for creating a new processing job and appending it to the job queue
        '''
        ...

if __name__ == "__main__":
    ingestor = ArXivIngestor()
    d = ingestor.search(["all:electron"], max_results=1)
    print(d)
    url = d["entries"][0]['id']
    url = url[:17] + 'html' + url[20:]
    ingestor.read(url)

