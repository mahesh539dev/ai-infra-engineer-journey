import logging
from core.measure_latency import track_latency
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception
from pinecone.exceptions import PineconeApiException, NotFoundException
from pinecone.errors.exceptions import ApiError
import db.pinecone_setup as pinecone_setup

logger = logging.getLogger(__name__)

batch_size = 100
DEFAULT_INDEX = "week3"
DEFAULT_NAMESPACE = "default"


def _upsert_data(vectors_to_upsert, index_name: str, namespace: str):
    index = pinecone_setup.get_index(index_name)
    for i in range(0, len(vectors_to_upsert), batch_size):
        batch = vectors_to_upsert[i:i+batch_size]
        try:
            with track_latency("pinecone_upsert", request_id=None, record_count=len(batch)):
                index.upsert(vectors=batch, namespace=namespace)
        except NotFoundException:
            logger.error("index missing — reinitializing", extra={"index": index_name})
            pinecone_setup._index_cache.pop(index_name, None)
            raise


@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(min=1, max=5),
    retry=retry_if_exception(lambda e: isinstance(e, PineconeApiException) and e.status >= 500)
)
def upsert(vectors_to_upsert, index_name: str = DEFAULT_INDEX, namespace: str = DEFAULT_NAMESPACE):
    try:
        _upsert_data(vectors_to_upsert, index_name, namespace)
    except ApiError as e:
        logger.error("pinecone upsert failed", extra={"status": e.status_code, "message": str(e)})
        raise
    except PineconeApiException as e:
        logger.error("pinecone upsert failed", extra={"message": str(e)})
        raise
    except Exception as e:
        logger.error("unexpected error", extra={"error": str(e), "type": type(e).__name__})
        raise
