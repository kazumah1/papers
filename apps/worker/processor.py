import json
from bs4 import BeautifulSoup
from infra.postgres import _postgres_db, _images_db, _vector_db, test_tables, db_get_paper, drop_table
from infra.gcs import upload_figure
from apps.llm import OpenAIClient
from utils.utils import Colors
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

DB = _postgres_db()
VECTOR_DB = _vector_db()
IMAGE_DB = _images_db()
register_vector(VECTOR_DB)

OPENAI_CLIENT = OpenAIClient()
MODEL = SentenceTransformer("nomic-ai/nomic-embed-text-v1", trust_remote_code=True)

chunk_prefix_length = 17
CONTEXT_LENGTH = 2048 - chunk_prefix_length # length of 'search_document: '

def embed(serialized_job):
    '''
    generating embeddings with sentencetransformers then storing embeddings + metadata in pgvector vector db 
    for semantic search and later rag
    '''
    global CONTEXT_LENGTH, MODEL, VECTOR_DB
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
    line = 'search_document: '
    curr_len = len(line)

    # curr_chunk = 0
    # while curr_chunk + context_length <= len(full_text):
    #     text_chunks.append('search document: ' + full_text[curr_chunk:curr_chunk+context_length])
    #     curr_chunk += context_length - ((self.context_length + self.chunk_prefix_length) // 8)
    # if curr_chunk < len(full_text):
    #     text_chunks.append('search document: ' + full_text[curr_chunk:])
    for s in sentences:
        if curr_len + len(s) > CONTEXT_LENGTH:
            text_chunks.append(line)
            temp_s = s
            if len(temp_s) >= CONTEXT_LENGTH:
                while len(temp_s) >= CONTEXT_LENGTH:
                    line = 'search_document: '
                    line += temp_s[:CONTEXT_LENGTH]
                    text_chunks.append(line)
                    temp_s = temp_s[CONTEXT_LENGTH:]
            line = 'search_document: '
            curr_len = len(line)
            line += temp_s
            curr_len += len(temp_s)
        else:
            line += s 
            curr_len += len(s)
    embeddings = MODEL.encode(text_chunks)
    # for chunk in text_chunks:
    #    print(chunk)
    #    print()
    for e in embeddings:
        VECTOR_DB.execute(
                "INSERT INTO vectors (external_id, embedding) values (%s, %s)",
                (job['id'], e,)
        )

    print(f"{Colors.GREEN}Successfully embedded paper content{Colors.WHITE}")

def figures(serialized_job):
    global IMAGE_DB
    job = json.loads(serialized_job)
    
    paper_id = job['id']
    pdf_url = job['pdf_url']
    response = requests.get(pdf_url)
    filestream = BytesIO(response.content)
    pdf = pymupdf.open(stream=filestream)

    records = db_get_paper(paper_id)

    if not records:
        raise ValueError("No paper with given id")
        return False
    elif len(records) > 1:
        print(f"{Colors.YELLOW}More than one paper found{Colors.WHITE}")

    img_count = 0
    paper = records[0]
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
                    IMAGE_DB.execute(
                        "INSERT INTO images (blob_url, paper_id, caption) values (%s, %s, %s)",
                        (file_destination, paper_table_id, "")
                    )
                    IMAGE_DB.commit()

                img_count += 1
    print(f"{Colors.GREEN}Successfully stored figures{Colors.WHITE}")


def summarize(serialized_job):
    global OPENAI_CLIENT, DB
    job = json.loads(serialized_job)
    html_url = job['html_url']
    paper_id = job['id']
    text, paper_abstract = read_and_get_abstract(html_url)
    print(f"Abstract: {paper_abstract[:100]}")

    records = db_get_paper(paper_id)
    if not records:
        raise ValueError("Error summarizing paper: No paper with given id")
        return False
    elif len(records) > 1:
        print(f"{Colors.YELLOW}More than one paper found{Colors.WHITE}")

    paper = records[0]
    if paper:
        DB.execute(f"""
            UPDATE papers
            SET abstract = '{paper_abstract}'
            WHERE external_id = '{paper_id}';
        """)
        print(f"{Colors.GREEN}Successfully extracted abstract{Colors.WHITE}")
        try:
            summary_text = OPENAI_CLIENT.summarize(text)
            DB.execute(f"""
                UPDATE papers
                SET summary = '{summary_text}'
                WHERE external_id = '{paper_id}';
            """)
            print(f"{Colors.GREEN}Successfully summarized paper{Colors.WHITE}")
            DB.commit()
        except Exception as e:
            DB.commit()
            raise ValueError("Error saving summary")

    return True



def read_and_get_abstract(html_url):
    '''
    for returning the raw html/xhtml of the paper html link
    args:
        url: url for the html page
    '''
    # don't like that this takes in url not page
    print(f"{Colors.YELLOW}html_url = {html_url}{Colors.WHITE}")
    response = requests.get(html_url)
    content = response.text
    soup = BeautifulSoup(content, 'html.parser')
    abstract_header = soup.find('h6', string="Abstract")
    abstract_content = abstract_header.find_next_sibling("p")
    abstract_div = soup.find('div', class_="ltx_abstract")
    abstract_text = abstract_div.get_text()[8:]
    return " ".join(soup.get_text().split()), abstract_text


def keywords(serialized_job):
    global DB
    job = json.loads(serialized_job)
    html_url = job['html_url']
    paper_id = job['id']
    records = db_get_paper(paper_id)
    if not records:
        raise ValueError("Error getting keywords: No paper with given id")
        return False
    elif len(records) > 1:
        print(f"{Colors.YELLOW}More than one paper returned{Colors.WHITE}")
    paper = records[0]
    if paper:
        print(paper)
        title = paper['title']
        abstract = paper['abstract']
        if title is None or abstract is None:
            raise ValueError("Error getting keywords: no title or abstract")
        text = title + abstract
        DB.execute(f"""
            UPDATE papers
            SET search_tsv = to_tsvector('english', '{text}')
            WHERE external_id = '{paper_id}';
        """)
        # DB.commit()
    print("Successfully stored keywords")
    return True


if __name__ == "__main__":
    # test_tables()
    # drop_table("papers")
    job = {
            "id": "arxiv.2511.11551",
            "pdf_url": "https://arxiv.org/pdf/2511.11551",
            "html_url": "https://arxiv.org/html/2511.11551",
    }
    print(summarize(json.dumps(job)))
    job2 = {
            "id":"arxiv.2512.22121v1",
            "pdf_url": "https://arxiv.org/pdf/2512.22121v1",
            "html_url": "https://arxiv.org/html/2512.22121v1",
    }
    print(summarize(json.dumps(job2)))
    # print(keywords(json.dumps(job)))
    # figures(json.dumps(job))
