from dotenv import load_dotenv
import psycopg
import os

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
                tags TEXT[],
                published_at TIMESTAMP NOT NULL
            )
        """)
        conn.commit()
    return conn

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
    conn = psycopg.connect(dbname=db_name, user=db_user, password=db_pass, host="localhost", port=5433)
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
    return conn


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
    conn = psycopg.connect(dbname=db_name, user=db_user, password=db_pass, host="localhost", port=5433)
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
        conn.execute("CREATE EXTENSION IF NOT EXISTS vector")
        conn.execute("""
            CREATE TABLE IF NOT EXISTS vectors (
                id BIGSERIAL PRIMARY KEY,
                external_id TEXT NOT NULL,
                embedding vector(768) NOT NULL
            )
        """)
        conn.execute('CREATE INDEX ON vectors USING hnsw (embedding vector_l2_ops)')
        conn.commit()
    return conn


def db_get_paper(paper_id):
    conn = _postgres_db()
    curr = conn.cursor()
    curr.execute(f"""
        SELECT * FROM papers WHERE external_id = '{paper_id}'
    """)
    for record in curr:
        print(record)
    for record in curr:
        return record

def db_add(metadata):
    # TODO: verify metadata is in right format
    conn = _postgres_db()
    # TODO: ensure metadata has this format: (id, title, author, pdf_url, html_url, current_datetime)
    conn.execute(
        f"""INSERT INTO Papers 
            (id, external_id, title, author, pdf_url, html_url, content_hash, created_at) 
            VALUES 
            (%(id)s, %(external_id)s, %(title)s, %(author)s, %(pdf_url)s, %(html_url)s, %(content_hash)s, %(created_at)s)
        """,
        metadata
    )
    return True

def test_tables():
    conn = _postgres_db()
    curr = conn.cursor()
    print("papers table")
    curr.execute("""
        SELECT COUNT (DISTINCT id) FROM papers
    """)
    print("length:")
    for record in curr:
        print(record)

    curr.execute("""
        SELECT * FROM papers
    """)

    for record in curr:
        print(record)
    
    conn_images = _images_db()
    curr_images = conn_images.cursor()
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
    
    for record in curr_images:
        print(record)

    conn_vectors = _vector_db()
    curr_vectors = conn_vectors.cursor()
    print("vectors table")
    curr.execute("""
        SELECT COUNT (DISTINCT id) FROM images
    """)
    print("length:")
    for record in curr:
        print(record)

    curr_vectors.execute("""
        SELECT * FROM vectors
    """)
    for record in curr_vectors:
        print(record)

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
    conn = psycopg.connect(dbname=db_name, user=db_user, password=db_pass, host="localhost", port=5433)
    conn.execute(f"""
        DROP TABLE IF EXISTS {table_name}
    """)
    conn.commit()
    return True
