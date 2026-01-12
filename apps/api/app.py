from fastapi import FastAPI
from pydantic import BaseModel
from typing import Union, Optional, List
from datetime import datetime # no idea the name of the library or how to get a timestamp compatible datetime


app = FastAPI("papers api server")

@app.get("/health")
def health():
    return {"status" : "OK"}


@app.get("/search/{search_query}")
def search(search_query: str, date_from: Optional[timestamp], date_to: Optional[timestamp], tags: Optional[List[str]]):
    return {"search_query" : search_query}

