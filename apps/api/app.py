from fastapi import FastAPI
from pydantic import BaseModel
from typing import Union, Optional, List
from datetime import datetime
from apps.api.helpers import get_sorted_results, fetch_papers_from_ids


app = FastAPI("papers api server")

@app.get("/health")
def health():
    return {"status" : "OK"}


@app.get("/search/{search_query}")
def search(search_query: str, date_from: Optional[timestamp], date_to: Optional[timestamp], tags: Optional[List[str]]):
    result_ids = get_sorted_results(query=search_query, date_from=date_from, date_to=date_to, tags=tags)

    results = fetch_papers_from_ids(result_ids)

    return {
            "results":results
    }

