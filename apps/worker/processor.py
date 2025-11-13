import json
from ...infra.postgres import _postgres_db

class Processor:
    def __init__(self):
        self.conn = _postgres_db()
    
    def embed(self, serialized_job):
        job = json.dumps(serialized_job)
    
    def figures(self, serialized_job):
        job = json.dumps(serialized_job)
    
    def summarize(self, serialized_job):
        job = json.dumps(serialized_job)
    
    def keywords(self, serialized_job):
        job = json.dumps(serialized_job)
