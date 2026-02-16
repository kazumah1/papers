import json
from dotenv import load_dotenv
from pgvector.psycopg import register_vector
import psycopg
from psycopg.rows import dict_row
import os
import numpy as np

def new_conn():
    load_dotenv()
    if os.getenv("DEVELOPMENT") == "true":
        db_name = os.getenv("POSTGRES_DB_DEV")
        db_user = os.getenv("POSTGRES_USER_DEV")
        db_pass = os.getenv("POSTGRES_PASSWORD_DEV")
    else:
        db_name = os.getenv("POSTGRES_DB_PROD")
        db_user = os.getenv("POSTGRES_USER_PROD")
        db_pass = os.getenv("POSTGRES_PASSWORD_PROD")
    return psycopg.connect(dbname=db_name, user=db_user, password=db_pass, host="localhost", port=5433, row_factory=dict_row)


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
    with psycopg.connect(dbname=db_name, user=db_user, password=db_pass, host="localhost", port=5433, row_factory=dict_row) as conn:
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
            # might need to add gcs link to pdf and abstract
            conn.execute("""
                CREATE TABLE IF NOT EXISTS Papers (
                    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                    external_id TEXT NOT NULL UNIQUE,
                    source TEXT NOT NULL,
                    title TEXT NOT NULL,
                    authors TEXT[] NOT NULL,
                    pdf_url TEXT NOT NULL,
                    html_url TEXT NOT NULL,
                    content_hash BYTEA NOT NULL UNIQUE,
                    abstract TEXT,
                    summary TEXT,
                    search_tsv tsvector,
                    tags TEXT[],
                    published_at TIMESTAMP NOT NULL
                )
            """)
            conn.commit()
    return True

def _images_db():
    load_dotenv()
    if os.getenv("DEVELOPMENT") == "true":
        db_name = os.getenv("POSTGRES_DB_DEV")
        db_user = os.getenv("POSTGRES_USER_DEV")
        db_pass = os.getenv("POSTGRES_PASSWORD_DEV")
    else:
        db_name = os.getenv("POSTGRES_DB_PROD")
        db_user = os.getenv("POSTGRES_USER_PROD")
        db_pass = os.getenv("POSTGRES_PASSWORD_PROD")
    with psycopg.connect(dbname=db_name, user=db_user, password=db_pass, host="localhost", port=5433, row_factory=dict_row) as conn:
        conn.execute("""
            SELECT EXISTS (
                SELECT FROM information_schema.tables
                WHERE table_name = 'images'
            )
        """)
        try:
            table_exists = conn.cursor().fetchone()
        except Exception:
            table_exists = None
        if not table_exists:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS Images (
                    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                    blob_url TEXT NOT NULL,
                    paper_id UUID NOT NULL,
                    caption TEXT
                )
            """)
            conn.commit()
    return True


def _vector_db():
    load_dotenv()
    if os.getenv("DEVELOPMENT") == "true":
        db_name = os.getenv("POSTGRES_DB_DEV")
        db_user = os.getenv("POSTGRES_USER_DEV")
        db_pass = os.getenv("POSTGRES_PASSWORD_DEV")
    else:
        db_name = os.getenv("POSTGRES_DB_PROD")
        db_user = os.getenv("POSTGRES_USER_PROD")
        db_pass = os.getenv("POSTGRES_PASSWORD_PROD")
    with psycopg.connect(dbname=db_name, user=db_user, password=db_pass, host="localhost", port=5433, row_factory=dict_row) as conn:
        conn.execute("CREATE EXTENSION IF NOT EXISTS vector")
        register_vector(conn)
        conn.execute("""
            SELECT EXISTS (
                SELECT FROM information_schema.tables
                WHERE table_name = 'vectors'
            )
        """)
        # tables = conn.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'")
        # if not tables:
        #     print('no tables found')
        # else:
        #     for table in tables:
        #         print(table)
        #         columns = conn.execute(f"SELECT column_name FROM information_schema.columns WHERE table_name = '{table[0]}' AND table_schema = 'public'")
        #         for c in columns:
        #             print(c)

        try:
            table_exists = conn.cursor().fetchone()
        except Exception:
            table_exists = None
        if not table_exists:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS vectors (
                    id BIGSERIAL PRIMARY KEY,
                    external_id TEXT NOT NULL,
                    embedding vector(768) NOT NULL
                )
            """)
            conn.execute('CREATE INDEX ON vectors USING hnsw (embedding vector_l2_ops)')
            conn.commit()
    return True


def db_get_paper(paper_id):
    records = []
    with new_conn() as conn:
        curr = conn.cursor()
        curr.execute("SELECT * FROM papers WHERE external_id = %s",
        (paper_id,))
        for record in curr:
            records.append(record)
    return records

def db_search_by_pdf_url(pdf_url):
    records = []
    with new_conn() as conn:
        curr = conn.cursor()
        curr.execute("""
            SELECT * FROM papers WHERE pdf_url = %s
        """,
        (pdf_url,))
        for record in curr:
            records.append(record)
    return records

def db_get_entry(entry_id):
    records = []
    with new_conn() as conn:
        curr = conn.cursor()
        curr.execute("""
            SELECT * FROM papers WHERE id = %s
        """,
        (entry_id,))
        for record in curr:
            records.append(record)
    print(len(records))
    return records.pop()

def db_semantic_search(query_embeddings):
    records = []
    paper_records = []
    with new_conn() as conn:
        for e in query_embeddings:
            curr = conn.cursor()
            curr.execute("""
                SELECT * FROM vectors ORDER BY embedding <-> %s LIMIT 25;
            """,
            (e.tolist(),))
            for record in curr:
                records.append(record)
        for record in records:
            paper_id = record['external_id']
            paper_rs = db_get_paper(paper_id)
            for r in paper_rs:
                r['embedding'] = np.asarray(json.loads(record['embedding']))
                paper_records.append(r)
    return paper_records

def db_keyword_search(keywords: list):
    records = []
    paper_records = []
    with new_conn() as conn:
        curr = conn.cursor()
        curr.execute("""
            SELECT DISTINCT ON (external_id) * FROM papers WHERE search_tsv @@ to_tsquery(%s) ORDER BY external_id, published_at DESC LIMIT 25;
        """,
        (" | ".join(keywords),))
        for record in curr:
            records.append(record)

        for record in records:
            paper_id = record['external_id']
            paper_rs = db_get_paper(paper_id)
            for r in paper_rs:
                record['embedding'] = np.asarray(json.loads(r['embedding']))
                paper_records.append(record)
    return paper_records

def db_add(metadata):
    # TODO: verify metadata is in right format
    with new_conn() as conn:
        # TODO: ensure metadata has this format: (id, title, author, pdf_url, html_url, current_datetime)
        conn.execute(
            """INSERT INTO Papers 
                (id, external_id, title, author, pdf_url, html_url, content_hash, created_at) 
                VALUES 
                (%(id)s, %(external_id)s, %(title)s, %(author)s, %(pdf_url)s, %(html_url)s, %(content_hash)s, %(created_at)s)
            """,
            metadata
        )
    return True

def test_tables():
    with new_conn() as conn:
        curr = conn.cursor()
        print("papers table")
        curr.execute("""
            SELECT COUNT (id) FROM papers
        """)
        print("length:")
        for record in curr:
            print(record)

        curr.execute("""
            SELECT * FROM papers
        """)

        # for record in curr:
        #     print(record)
        
        curr_images = conn.cursor()
        print("images table")
        curr.execute("""
            SELECT COUNT (DISTINCT id) FROM images
        """)
        print("length:")
        for record in curr:
            print(record)

        curr_images.execute("""
            SELECT * FROM images
        """)
        
        # for record in curr_images:
        #     print(record)

        curr_vectors = conn.cursor()
        print("vectors table")
        curr.execute("""
            SELECT COUNT (DISTINCT id) FROM vectors
        """)
        print("length:")
        for record in curr:
            print(record)

        curr_vectors.execute("""
            SELECT * FROM vectors
        """)
        for record in curr_vectors:
            print(record['external_id'])
    return True

def drop_table(table_name):
    if table_name not in {"papers", "vectors", "images"}:
        return False
    load_dotenv()
    if os.getenv("DEVELOPMENT") == "true":
        db_name = os.getenv("POSTGRES_DB_DEV")
        db_user = os.getenv("POSTGRES_USER_DEV")
        db_pass = os.getenv("POSTGRES_PASSWORD_DEV")
    else:
        db_name = os.getenv("POSTGRES_DB_PROD")
        db_user = os.getenv("POSTGRES_USER_PROD")
        db_pass = os.getenv("POSTGRES_PASSWORD_PROD")
    try:
        with new_conn() as conn:
            conn.execute("SET lock_timeout = '5s'")
            conn.execute("""
                DROP TABLE IF EXISTS %s
            """,
            (table_name,))
            conn.commit()
    except Exception as e:
        print("Error dropping table:", e)
        return False
    return True
