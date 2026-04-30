from fastapi import FastAPI
from fastapi.responses import JSONResponse
from contextlib import asynccontextmanager
import chroma_actions

@asynccontextmanager
async def lifespan(app: FastAPI):
    # runs on startup
    chroma_actions.load_data_to_chroma()
    yield
    # runs on shutdown (optional cleanup)

app = FastAPI(lifespan=lifespan)

@app.get("/search-semantic")
def search_query_in_chroma(query_text: str, num_results: int = 5):
    results = chroma_actions.query_data([query_text], num_results)
    if not results:
        return JSONResponse(status_code=404, content={"message": "No results found"})
    return JSONResponse(status_code=200, content={"results": results})
