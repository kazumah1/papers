import json
from bs4 import BeautifulSoup
from infra.postgres import _postgres_db, _images_db, _vector_db, test_tables, db_get_paper
from infra.gcs import upload_figure
from apps.llm import OpenAIClient
from sentence_transformers import SentenceTransformer
from PyPDF2 import PdfReader 
from urllib.request import Request, urlopen
from io import BytesIO
from pgvector.psycopg import register_vector
import numpy as np
import re
import pymupdf
from PIL import Image
import requests

class Processor:
    def __init__(self):
        self.db = _postgres_db()
        self.image_db = _images_db()
        self.vector_db = _vector_db()
        register_vector(self.vector_db)
        self.openai_client = OpenAIClient()
        self.model = SentenceTransformer("nomic-ai/nomic-embed-text-v1", trust_remote_code=True)
        self.chunk_prefix_length = 17
        self.context_length = 8192 - self.chunk_prefix_length # length of 'search document: '
    
    def embed(self, serialized_job):
        '''
        generating embeddings with sentencetransformers then storing embeddings + metadata in pgvector vector db 
        for semantic search and later rag
        '''
        job = json.loads(serialized_job)
        

        pdf_url = job['pdf_url']
        response = requests.get(pdf_url)
        memfile = BytesIO(response.content)
        reader = PdfReader(memfile)


        full_text = ""
        for p in reader.pages:
            full_text += p.extract_text()

        full_text = full_text.replace("-\n", "")
        full_text = full_text.replace("\n", " ")
        sentences = re.split(r'(?<=\.)', full_text)
        
        text_chunks = []
        line = 'search document: '
        curr_len = len(line)

        # curr_chunk = 0
        # while curr_chunk + self.context_length <= len(full_text):
        #     text_chunks.append('search document: ' + full_text[curr_chunk:curr_chunk+self.context_length])
        #     curr_chunk += self.context_length - ((self.context_length + self.chunk_prefix_length) // 8)
        # if curr_chunk < len(full_text):
        #     text_chunks.append('search document: ' + full_text[curr_chunk:])
        for s in sentences:
            if curr_len + len(s) > self.context_length:
                text_chunks.append(line)
                line = 'search document: '
                curr_len = len(line)
            line += s 
            curr_len += len(s)
        embeddings = self.model.encode(text_chunks)
        # for chunk in text_chunks:
        #    print(chunk)
        #    print()
        for e in embeddings:
            self.vector_db.execute(
                    "INSERT INTO vectors (external_id, embedding) values (%s, %s)",
                    (job['id'], e,)
            )

    def figures(self, serialized_job):
        job = json.loads(serialized_job)
        
        paper_id = job['id']
        pdf_url = job['pdf_url']
        response = requests.get(pdf_url)
        filestream = BytesIO(response.content)
        pdf = pymupdf.open(stream=filestream)

        paper = db_get_paper(paper_id)
        if not paper or paper is None:
            print("No paper with given id")
        else:
            print("paper found")

        img_count = 0
        if paper:
            for page in pdf:
                images = page.get_images()
                for img_idx, image in enumerate(images):
                    xref = image[0]
                    img = pdf.extract_image(xref)
                    img_bytes = BytesIO(img['image'])
                    file_destination = paper_id + '/' + str(img_count)
                    upload_figure(img_bytes, file_destination) 
                    
                    if paper:
                        paper_table_id = paper["id"]
                        self.image_db.execute(
                            "INSERT INTO images (blob_url, paper_id, caption) values (%s, %s, %s)",
                            (file_destination, paper_table_id, "")
                        )
                        self.image_db.commit()

                    img_count += 1

    
    def summarize(self, serialized_job):
        job = json.loads(serialized_job)
        html_url = job['html_url']
        paper_id = job['id']
        text = self.read(html_url)
        print(f"text sample : {text[:250]}")
        summary_text = self.openai_client.summarize(text)
        # need to extract Abstract by parsing

        paper = db_get_paper(paper_id)
        if not paper or paper is None:
            raise ValueError("Error summarizing paper: No paper with given id")
            return False
        else:
            print("paper found for summarize")

        if paper:
            self.db.execute(f"""
                UPDATE papers
                SET summary = '{summary_text}'
                WHERE external_id = {paper_id};
            """)
            self.db.commit()
        return True



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
        return " ".join(soup.get_text().split())

    
    def keywords(self, serialized_job):
        job = json.loads(serialized_job)

if __name__ == "__main__":
    # test_tables()
    processor = Processor()
    job = {
            "id": "arxiv.2511.11551",
            "pdf_url": "https://arxiv.org/pdf/2511.11551",
            "html_url": "https://arxiv.org/html/2511.11551",
    }
    print(processor.summarize(json.dumps(job)))
    # processor.figures(json.dumps(job))
