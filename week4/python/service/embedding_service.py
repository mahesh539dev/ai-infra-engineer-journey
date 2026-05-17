from sentence_transformers import SentenceTransformer
import logging
from core.measure_latency import track_latency
from db import pinecone_insertion, pinecone_retrieval

logger = logging.getLogger(__name__)

MAX_TOKENS = 512


class EmbeddingService:

    def __init__(self, model_name: str = 'all-MiniLM-L6-v2'):
        try:
            self.model = SentenceTransformer(model_name)
            logger.info("model loaded", extra={"model": model_name})
        except Exception as e:
            logger.error("failed to load model", extra={"error": str(e)})
            raise

    def _safe_encode(self, message: str) -> list:
        if not message or not message.strip():
            raise ValueError("empty message — skipping encode")
        tokens = self.model.tokenizer(message, truncation=False)["input_ids"]
        if len(tokens) > MAX_TOKENS:
            logger.warning("message truncated", extra={"original_tokens": len(tokens), "max_tokens": MAX_TOKENS})
            message = self.model.tokenizer.decode(tokens[:MAX_TOKENS], skip_special_tokens=True)
        return self.model.encode(message).tolist()

    def _encode_batch(self, texts: list[str]) -> list:
        validated = []
        for t in texts:
            if not t or not t.strip():
                raise ValueError("empty message — skipping encode")
            tokens = self.model.tokenizer(t, truncation=False)["input_ids"]
            if len(tokens) > MAX_TOKENS:
                logger.warning("message truncated", extra={"original_tokens": len(tokens), "max_tokens": MAX_TOKENS})
                t = self.model.tokenizer.decode(tokens[:MAX_TOKENS], skip_special_tokens=True)
            validated.append(t)
        return self.model.encode(validated).tolist()

    def upsert_data(self, eventList, partitions, offsets):
        if not eventList:
            logger.warning("upsert_data called with empty list — skipping")
            return
        assert len(eventList) == len(partitions) == len(offsets), "eventList, partitions and offsets must be the same length"
        with track_latency("encode_embeddings", request_id=None, record_count=len(eventList)):
            embeddings = self._encode_batch([e.text for e in eventList])
            vectors_to_upsert = [
                {
                    "id": f"{event.topic}-{partition}-{offset}",
                    "values": embedding,
                    "metadata": {
                        "topic": event.topic,
                        "subtopic": event.subtopic,
                        "message": event.message
                    }
                }
                for event, partition, offset, embedding in zip(eventList, partitions, offsets, embeddings)
            ]
            with track_latency("pinecone_insertion", request_id=None, record_count=len(vectors_to_upsert)):
                pinecone_insertion.upsert(vectors_to_upsert)
            logger.info("Inserted Data Successfully")

    def semantic_search_with_query(self, query,index_name):
        if not query:
            logger.warning("Search text cannot be empty. Please provide search query.")
            return
        embeddings = self._safe_encode(query)
        results = pinecone_retrieval.pinecone_search_semantic(embeddings,index_name)
        if not (results['matches']):
            logger.warning("No reults found for specific query. Try a different one")
            return
        return [match.to_dict() for match in results['matches']]

    def upsert_rag_data(self, eventList, partitions, offsets):
        if not eventList:
            logger.warning("upsert_data called with empty list — skipping")
            return
        assert len(eventList) == len(partitions) == len(offsets), "eventList, partitions and offsets must be the same length"
        with track_latency("encode_embeddings", request_id=None, record_count=len(eventList)):
            embeddings = self._encode_batch([e.text for e in eventList])
            vectors_to_upsert = [
                {
                    "id": f"{event.document.document_id}-{event.chunk.chunk_index}",
                    "values": embedding,
                    "metadata": {
                        "document_id": event.document.document_id,
                        "document_name": event.document.document_name,
                        "chunk_index": event.chunk.chunk_index,
                        "text": event.chunk.text,
                        "tags": event.metadata.tags,
                        "language": event.metadata.language
                    }
                }
                for event, _, _, embedding in zip(eventList, partitions, offsets, embeddings)
            ]
            index_name = eventList[0].pinecone.index_name
            namespace = eventList[0].pinecone.namespace
            with track_latency("pinecone_insertion", request_id=None, record_count=len(vectors_to_upsert)):
                pinecone_insertion.upsert(vectors_to_upsert, index_name=index_name, namespace=namespace)
            logger.info("Inserted Data Successfully")
