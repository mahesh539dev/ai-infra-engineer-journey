from pinecone import Pinecone
from dotenv import load_dotenv
import os
import time
import logging

load_dotenv()
logger = logging.getLogger(__name__)

pinecone_key = os.getenv("PINECONE_API_KEY")
if not pinecone_key:
    raise EnvironmentError("PINECONE_API_KEY is not set")

pc = Pinecone(api_key=pinecone_key)
index_name = "week3"

if not pc.has_index(index_name):
    pc.create_index(
        name=index_name,
        dimension=384,
        metric="cosine",
        spec={
            "serverless": {
                "cloud": "aws",
                "region": "us-east-1"
            }
        }
    )

while not pc.describe_index(index_name).status.ready:
    time.sleep(1)
logger.info("✅ Index is ready")

index = pc.Index(index_name)

_index_cache: dict = {}

def get_index(name: str):
    if name not in _index_cache:
        if not pc.has_index(name):
            raise ValueError(f"Pinecone index '{name}' does not exist")
        _index_cache[name] = pc.Index(name)
        logger.info("index connection created", extra={"index": name})
    return _index_cache[name]
