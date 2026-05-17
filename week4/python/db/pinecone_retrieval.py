import logging
from core.measure_latency import track_latency
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception
from pinecone.exceptions import PineconeApiException, NotFoundException
import db.pinecone_setup as pinecone_setup
from db.pinecone_setup import pc, index_name

#Semantic Search
def pinecone_search_semantic(vectorList, index_name, namespace: str = "file-data"):
    index = pinecone_setup.get_index(index_name)
    results = index.query(
        vector=vectorList,
        top_k=5,
        include_metadata=True,
        include_values=False,
        namespace=namespace
    )
    return results
    