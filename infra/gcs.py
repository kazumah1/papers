from google.cloud.storage import Client
import os
from typing import List

def upload_figure(file, destination_blob_name):
    bucket_name = os.getenv("GCS_FIGURE_BUCKET_NAME")
    storage_client = Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob("figures/" + destination_blob_name + '.jpg')

    file.seek(0)
    blob.upload_from_file(file)

    print(f"File uploaded to {destination_blob_name}")

