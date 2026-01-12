### api server helper functions
from typing import Optional, List
from sentence_transformers import SentenceTransformer
from infra.postgres import new_conn, db_paper_search
from pgvector.psycopg import register_vector

RECENCY_WEIGHT = None
RELEVANCE_WEIGHT = None
QUALITY_WEIGHT = None

MODEL = SentenceTransformer("nomic-ai/nomic-embed-text-v1", trust_remote_code=True)

def get_highest_rank(query: str, date_from: Optional[timestamp], date_to: Optional[timestamp], tags: Optional[List[str]]):
    """the ranking heuristic is a weighted product of 3 components:
        recency : exponential decay, prioritizing more recent papers
        relevance : a weighted sum of semantic (user profile) and keyword matching (search query)
        quality : a rough estimation of paper quality based on abstract length and keywords. ideally train a small model for this later
    """
    global MODEL
    # TODO: DONT HAVE USER PROFILE RN
    # TODO: get rid of heuristic in place of model once enough papers in corpus and once working version is done

    # candidate id : (recency, relevance, quality, overall)
    candidates = {}
    
    text_chunks = []
    text_chunks.append("search_query: " + query)
    # get top k candidates
    embeddings = MODEL.encode(text_chunks)
    
    records = db_paper_search(embeddings)

    for record in records:
        print(record['title'])

    # for each candidate, calculate recency, relevance, quality, and overall scores


if __name__ == "__main__":
    get_highest_rank("quantum mechanics", None, None, None)
