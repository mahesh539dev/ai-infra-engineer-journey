# RAG Answer Generation — Design Spec
Date: 2026-05-10

## Overview

Add a `/ask` endpoint to the existing FastAPI app that performs full RAG: semantic search against Pinecone, then LLM answer generation using OpenAI `gpt-4o-mini`. A new module `python/openai/rag_answer.py` handles the prompt construction and OpenAI call as a plain function.

## Architecture & Data Flow

```
GET /ask?query=...
      │
      ▼
data_apis.py  (/ask endpoint)
      │
      ├─► EmbeddingService.semantic_search_with_query(query)
      │         └─► Pinecone → returns list of match dicts
      │
      ├─► openai/rag_answer.py :: generate_answer(query, pinecone_results)
      │         └─► builds prompt → OpenAI gpt-4o-mini → returns answer string
      │
      └─► JSONResponse {"answer": "..."}
```

## Components

### New: `python/openai/rag_answer.py`

- Module-level `OpenAI` client initialized from `OPENAI_API_KEY` env var (same pattern as `async_call.py`)
- Single public function: `generate_answer(query: str, pinecone_results: list[dict]) -> str`
- Prompt construction:
  - Extracts `metadata["message"]` from each Pinecone match dict
  - Formats them as numbered context passages (1., 2., 3., ...)
  - System prompt: instructs model to answer only using the provided context; if the context doesn't contain the answer, say so
  - User message: context block followed by the user's question
- Model: `gpt-4o-mini`, `temperature=0.3` (low — factual retrieval context), `max_tokens=500`
- Returns the answer string from `response.choices[0].message.content`
- If `pinecone_results` is empty, returns fallback string: `"I could not find relevant information to answer your question."`

### Updated: `python/api/data_apis.py`

New endpoint `GET /ask`:
1. Validate `query` is non-empty — return 400 if missing
2. Call `embedding_service.semantic_search_with_query(query)` — reuses existing `EmbeddingService` instance
3. If no Pinecone results, return 404 `{"message": "No results found"}`
4. Call `generate_answer(query, results)` from `rag_answer`
5. Return 200 `{"answer": "<generated answer string>"}`

## What Is Not Changing

- `EmbeddingService` — reused as-is, no modifications
- `pinecone_retrieval.py` — no modifications
- `async_call.py` — remains a standalone learning script, untouched

## Prompt Template

```
System:
  You are a helpful assistant. Answer the user's question using only the
  context passages provided below. If the context does not contain enough
  information to answer, say "I don't have enough information to answer that."

User:
  Context:
  1. <message from match 1>
  2. <message from match 2>
  ...

  Question: <user query>
```

## Out of Scope

- Streaming responses
- Conversation history / multi-turn chat
- Model configurability at request time
- Async OpenAI calls
