from google.cloud.storage import Client
from utils.utils import Colors
import os
from typing import List

def upload_figure(file, destination_blob_name):
    storage_client = Client()
    bucket_name = os.getenv("GCS_BUCKET_NAME")
    bucket = storage_client.bucket(bucket_name)

    blob_name = f"figures/{destination_blob_name}.jpg"
    blob_exists = bucket.get_blob(blob_name)
    if blob_exists is not None:
        print(f"{Colors.PURPLE}Figure {destination_blob_name} already exists in GCS{Colors.WHITE}")
        return True
    blob = bucket.blob(blob_name)

    file.seek(0)
    blob.upload_from_file(file)

    print(f"{Colors.PURPLE}File uploaded to {destination_blob_name}{Colors.WHITE}")


def upload_paper(filename, content):
    storage_client = Client()
    bucket_name = os.getenv("GCS_BUCKET_NAME")
    bucket = storage_client.bucket(bucket_name)

    # whole point of hashing files
    blob_name = f"raw/{filename[:2]}/{filename[2:4]}/{filename}"
    blob_exists = bucket.get_blob(blob_name)
    if blob_exists is not None:
        print(f"{Colors.PURPLE}Paper already exists in GCS{Colors.WHITE}")
        return True

    blob = bucket.blob(blob_name)

    blob.upload_from_string(content, content_type="application/pdf")
    return True
