import logging
from fastapi import FastAPI, Query
from service.embedding_service import EmbeddingService
from llm.rag_answer import generate_answer
from fastapi.responses import JSONResponse
from core.logging_config import setup_logging

setup_logging()
logger = logging.getLogger(__name__)

app = FastAPI()
embedding_service = EmbeddingService()


@app.get("/semantic-search")
def semantic_retrieval(query_text: str):
    results = embedding_service.semantic_search_with_query(query_text)
    if not results:
        return JSONResponse(status_code=404, content={"message": "No results found"})
    return JSONResponse(status_code=200, content={"results": results})


@app.get("/ask-knowledge")
def ask(query: str = Query(..., min_length=1)):
    index_name = "infra-knowledge"
    results = embedding_service.semantic_search_with_query(query,index_name)
    if not results:
        return JSONResponse(status_code=404, content={"message": "No results found"})
    try:
        answer = generate_answer(query, results)
    except Exception as e:
        logger.error("LLM call failed", extra={"error": str(e)})
        return JSONResponse(status_code=502, content={"message": "Failed to generate answer"})
    return JSONResponse(status_code=200, content={"answer": answer})
