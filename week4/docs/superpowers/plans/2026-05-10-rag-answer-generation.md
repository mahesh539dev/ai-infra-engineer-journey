# RAG Answer Generation Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a `/ask` endpoint that takes a user query, retrieves semantically similar messages from Pinecone, and returns an LLM-generated answer using `gpt-4o-mini`.

**Architecture:** A new `python/openai/rag_answer.py` module exposes a single `generate_answer(query, pinecone_results)` function. The `/ask` endpoint in `data_apis.py` orchestrates the two steps: Pinecone retrieval via the existing `EmbeddingService`, then LLM answer generation via `generate_answer`.

**Tech Stack:** FastAPI, OpenAI Python SDK (`openai`), `python-dotenv`, `pytest`, `unittest.mock`

---

## File Map

| Action | Path | Responsibility |
|--------|------|----------------|
| Create | `python/openai/rag_answer.py` | Prompt construction + OpenAI call |
| Modify | `python/api/data_apis.py` | Add `/ask` endpoint |
| Create | `python/tests/__init__.py` | Make tests a package |
| Create | `python/tests/test_rag_answer.py` | Unit tests for `generate_answer` |
| Create | `python/tests/test_ask_endpoint.py` | Unit tests for `/ask` endpoint |

---

### Task 1: Create `rag_answer.py` with `generate_answer`

**Files:**
- Create: `python/openai/rag_answer.py`

- [ ] **Step 1: Write the failing test**

Create `python/tests/__init__.py` (empty file), then create `python/tests/test_rag_answer.py`:

```python
import pytest
from unittest.mock import MagicMock, patch


SAMPLE_RESULTS = [
    {"metadata": {"message": "Kafka is a distributed event streaming platform."}},
    {"metadata": {"message": "Pinecone is a vector database for similarity search."}},
]


@patch("openai.rag_answer.client")
def test_generate_answer_calls_openai_with_context(mock_client):
    from openai.rag_answer import generate_answer

    mock_response = MagicMock()
    mock_response.choices[0].message.content = "Kafka streams events; Pinecone stores vectors."
    mock_client.chat.completions.create.return_value = mock_response

    result = generate_answer("What is Kafka?", SAMPLE_RESULTS)

    assert result == "Kafka streams events; Pinecone stores vectors."
    call_kwargs = mock_client.chat.completions.create.call_args.kwargs
    assert call_kwargs["model"] == "gpt-4o-mini"
    messages = call_kwargs["messages"]
    assert messages[0]["role"] == "system"
    assert "context" in messages[1]["content"].lower()
    assert "What is Kafka?" in messages[1]["content"]
    assert "Kafka is a distributed event streaming platform." in messages[1]["content"]


@patch("openai.rag_answer.client")
def test_generate_answer_returns_fallback_when_no_results(mock_client):
    from openai.rag_answer import generate_answer

    result = generate_answer("What is Kafka?", [])

    mock_client.chat.completions.create.assert_not_called()
    assert result == "I could not find relevant information to answer your question."
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
cd c:/learning_journey/week4/python
python -m pytest tests/test_rag_answer.py -v
```

Expected: `ModuleNotFoundError: No module named 'openai.rag_answer'`

- [ ] **Step 3: Create `python/openai/rag_answer.py`**

```python
import os
from dotenv import load_dotenv
from openai import OpenAI

load_dotenv()
client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))

SYSTEM_PROMPT = (
    "You are a helpful assistant. Answer the user's question using only the "
    "context passages provided below. If the context does not contain enough "
    "information to answer, say \"I don't have enough information to answer that.\""
)


def generate_answer(query: str, pinecone_results: list[dict]) -> str:
    if not pinecone_results:
        return "I could not find relevant information to answer your question."

    context_passages = "\n".join(
        f"{i + 1}. {match['metadata']['message']}"
        for i, match in enumerate(pinecone_results)
    )
    user_message = f"Context:\n{context_passages}\n\nQuestion: {query}"

    response = client.chat.completions.create(
        model="gpt-4o-mini",
        messages=[
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user", "content": user_message},
        ],
        temperature=0.3,
        max_tokens=500,
    )
    return response.choices[0].message.content
```

- [ ] **Step 4: Run tests to verify they pass**

```bash
cd c:/learning_journey/week4/python
python -m pytest tests/test_rag_answer.py -v
```

Expected:
```
PASSED tests/test_rag_answer.py::test_generate_answer_calls_openai_with_context
PASSED tests/test_rag_answer.py::test_generate_answer_returns_fallback_when_no_results
```

- [ ] **Step 5: Commit**

```bash
git add python/openai/rag_answer.py python/tests/__init__.py python/tests/test_rag_answer.py
git commit -m "feat: add generate_answer function for RAG LLM response"
```

---

### Task 2: Add `/ask` endpoint to `data_apis.py`

**Files:**
- Modify: `python/api/data_apis.py`
- Create: `python/tests/test_ask_endpoint.py`

Current content of `python/api/data_apis.py`:
```python
from fastapi import FastAPI
from service.embedding_service import EmbeddingService
from fastapi.responses import JSONResponse

app = FastAPI()
embedding_service = EmbeddingService()

@app.get("/semantic-search")
def semantic_retrieval(query_text: str):
    results = embedding_service.semantic_search_with_query(query_text)
    if not results:
        return JSONResponse(status_code=404, content={"message": "No results found"})
    return JSONResponse(status_code=200, content={"results": results})
```

- [ ] **Step 1: Write the failing test**

Create `python/tests/test_ask_endpoint.py`:

```python
import pytest
from unittest.mock import MagicMock, patch
from fastapi.testclient import TestClient


SAMPLE_PINECONE_RESULTS = [
    {"metadata": {"message": "Kafka is a distributed event streaming platform."}},
]


@pytest.fixture()
def client():
    with patch("api.data_apis.EmbeddingService"), \
         patch("api.data_apis.generate_answer"):
        from api.data_apis import app
        return TestClient(app)


def test_ask_returns_answer(client):
    with patch("api.data_apis.embedding_service") as mock_svc, \
         patch("api.data_apis.generate_answer") as mock_gen:
        mock_svc.semantic_search_with_query.return_value = SAMPLE_PINECONE_RESULTS
        mock_gen.return_value = "Kafka streams events."

        response = client.get("/ask", params={"query": "What is Kafka?"})

    assert response.status_code == 200
    assert response.json() == {"answer": "Kafka streams events."}
    mock_svc.semantic_search_with_query.assert_called_once_with("What is Kafka?")
    mock_gen.assert_called_once_with("What is Kafka?", SAMPLE_PINECONE_RESULTS)


def test_ask_returns_404_when_no_pinecone_results(client):
    with patch("api.data_apis.embedding_service") as mock_svc, \
         patch("api.data_apis.generate_answer") as mock_gen:
        mock_svc.semantic_search_with_query.return_value = None

        response = client.get("/ask", params={"query": "What is Kafka?"})

    assert response.status_code == 404
    assert response.json() == {"message": "No results found"}
    mock_gen.assert_not_called()


def test_ask_returns_400_when_query_missing(client):
    response = client.get("/ask")
    assert response.status_code == 422
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
cd c:/learning_journey/week4/python
python -m pytest tests/test_ask_endpoint.py -v
```

Expected: Tests fail — `/ask` endpoint does not exist yet.

- [ ] **Step 3: Add `/ask` endpoint to `data_apis.py`**

Replace the full contents of `python/api/data_apis.py` with:

```python
from fastapi import FastAPI, Query
from service.embedding_service import EmbeddingService
from openai.rag_answer import generate_answer
from fastapi.responses import JSONResponse

app = FastAPI()
embedding_service = EmbeddingService()


@app.get("/semantic-search")
def semantic_retrieval(query_text: str):
    results = embedding_service.semantic_search_with_query(query_text)
    if not results:
        return JSONResponse(status_code=404, content={"message": "No results found"})
    return JSONResponse(status_code=200, content={"results": results})


@app.get("/ask")
def ask(query: str = Query(..., min_length=1)):
    results = embedding_service.semantic_search_with_query(query)
    if not results:
        return JSONResponse(status_code=404, content={"message": "No results found"})
    answer = generate_answer(query, results)
    return JSONResponse(status_code=200, content={"answer": answer})
```

- [ ] **Step 4: Run all tests to verify they pass**

```bash
cd c:/learning_journey/week4/python
python -m pytest tests/ -v
```

Expected:
```
PASSED tests/test_rag_answer.py::test_generate_answer_calls_openai_with_context
PASSED tests/test_rag_answer.py::test_generate_answer_returns_fallback_when_no_results
PASSED tests/test_ask_endpoint.py::test_ask_returns_answer
PASSED tests/test_ask_endpoint.py::test_ask_returns_404_when_no_pinecone_results
PASSED tests/test_ask_endpoint.py::test_ask_returns_400_when_query_missing
```

- [ ] **Step 5: Commit**

```bash
git add python/api/data_apis.py python/tests/test_ask_endpoint.py
git commit -m "feat: add /ask RAG endpoint wiring Pinecone search to LLM answer"
```

---

## Self-Review Checklist

- [x] **`rag_answer.py` module** — Task 1 covers creation, tests, and commit
- [x] **`generate_answer` signature** — `(query: str, pinecone_results: list[dict]) -> str` consistent across all tasks
- [x] **Empty results fallback** — covered in `test_generate_answer_returns_fallback_when_no_results`
- [x] **`/ask` endpoint** — Task 2 covers wiring, 200/404/422 cases, and commit
- [x] **Prompt template** — system prompt and user message match spec exactly
- [x] **`metadata["message"]` access** — consistent with `embedding_service.py:71` return shape
- [x] **Model/temperature/max_tokens** — `gpt-4o-mini`, `0.3`, `500` in implementation and verified in test
- [x] **No placeholders** — all steps contain complete code
