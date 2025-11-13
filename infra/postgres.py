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
        conn.execute("""
            CREATE EXTENSION IF NOT EXISTS 'pgcrypto'
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS Papers (
                id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                external_id STRING NOT NULL UNIQUE,
                source STRING NOT NULL,
                title STRING NOT NULL,
                authors STRING[] NOT NULL,
                pdf_url STRING NOT NULL,
                html_url STRING NOT NULL,
                content_hash BINARY(32) NOT NULL UNIQUE,
                summary TEXT,
                tags STRING[],
                published_at DATETIME NOT NULL
            )
        """)
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
        f"""INSERT INTO Papers 
            (id, external_id, title, author, pdf_url, html_url, content_hash, created_at) 
            VALUES 
            (%(id)s, %(external_id)s, %(title)s, %(author)s, %(pdf_url)s, %(html_url)s, %(content_hash)s, %(created_at)s)
        """,
        metadata
    )
    return True