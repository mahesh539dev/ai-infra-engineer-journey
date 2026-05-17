# Week 4: RAG Production Upgrade

> Clean Architecture · Structured Logging · Latency Tracking · Retry · RAG Answer Generation

An upgrade of the Week 3 Kafka consumer pipeline — refactored into a clean package structure and hardened with production-grade observability, retry logic, multi-topic threading, and a FastAPI RAG endpoint that answers questions using Pinecone + OpenAI.

---

## System Architecture

```
┌───────────────────────────────────────────────────────────────────┐
│                        Docker (Infrastructure)                    │
│   ┌──────────────────┐          ┌──────────────────────────────┐  │
│   │   Kafka Broker   │          │          Kafka UI             │  │
│   │   (port 9092)    │          │         (port 8080)           │  │
│   └────────┬─────────┘          └──────────────────────────────┘  │
└────────────┼──────────────────────────────────────────────────────┘
             │
       ┌─────┴──────┐
       │            │
  learning-ai   rag-events
       │            │
       ▼            ▼
┌──────────────────────────────────────────────────────┐
│              Python Service (main.py)                │
│                                                      │
│  kafka_consumer_service.py                           │
│    ├── Thread: learning-ai consumer                  │
│    │     └── EmbeddingService.upsert_data()          │
│    └── Thread: rag-events consumer                   │
│          └── EmbeddingService.upsert_rag_data()      │
│                                                      │
│  FastAPI (data_apis.py)                              │
│    ├── GET /semantic-search?query_text=X             │
│    └── GET /ask-knowledge?query=X                    │
│          ├── EmbeddingService.semantic_search()      │
│          └── generate_answer() → OpenAI gpt-4o-mini │
└──────────────────────────────────────────────────────┘
             │
             ▼
      Pinecone (serverless)
      index: week3 / infra-knowledge
```

---

## Data Flow

### Ingestion (Kafka → Pinecone)
1. Two consumer threads start from `TOPIC_CONFIGS` env var — one per topic
2. Each batch is dispatched to its topic handler (`_handle_learning_ai` or `_handle_rag_events`)
3. `EmbeddingService` token-checks and encodes text with `all-MiniLM-L6-v2` (384-dim)
4. Vectors are batch-upserted to Pinecone with retry on 5xx errors

### RAG Query (`/ask-knowledge`)
1. Query text is encoded to a vector
2. Top-5 Pinecone matches retrieved from `infra-knowledge` index
3. Context passages fed to OpenAI `gpt-4o-mini` with a strict system prompt
4. Answer returned to caller

---

## Package Structure

```
python/
├── main.py                      # entrypoint — setup_logging() + run_all_consumers()
├── api/
│   └── data_apis.py             # FastAPI: /semantic-search, /ask-knowledge
├── core/
│   ├── context.py               # ContextVar for request_id propagation
│   ├── logging_config.py        # JSON structured logging, stdout/stderr split
│   └── measure_latency.py       # track_latency() context manager
├── db/
│   ├── pinecone_setup.py        # Pinecone client + index cache
│   ├── pinecone_insertion.py    # upsert with tenacity retry
│   └── pinecone_retrieval.py    # semantic search (top-k=5)
├── llm/
│   ├── rag_answer.py            # OpenAI call with retry + system prompt
│   └── async_call.py            # standalone interactive LLM script
├── model/
│   ├── learning_ai_event.py     # Pydantic model for learning-ai topic
│   └── rag_event.py             # Pydantic model for rag-events topic (full provenance)
├── service/
│   ├── embedding_service.py     # encode + upsert + search orchestration
│   └── kafka_consumer_service.py # multi-topic threaded consumer runner
└── tests/
    ├── test_rag_answer.py        # unit tests for generate_answer()
    └── test_ask_endpoint.py      # FastAPI integration tests
```

---

## Infrastructure Setup

```bash
docker compose up -d
```

| Service | URL |
|---------|-----|
| Kafka broker | `localhost:9092` |
| Kafka UI | `http://localhost:8080` |
| Kafka REST Proxy | `http://localhost:8082` |

---

## Setup & Running

### 1. Create and activate virtual environment

```bash
python -m venv venv

# Windows
venv\Scripts\activate

# macOS/Linux
source venv/bin/activate
```

### 2. Install dependencies

```bash
pip install kafka-python pydantic sentence-transformers pinecone-client python-dotenv openai tenacity fastapi uvicorn
```

### 3. Configure environment variables

```env
TOPIC_CONFIGS=learning-ai:learning-ai-cg1,rag-events:rag-events-cg1
BROKERS=localhost:9092
PINECONE_API_KEY=your_pinecone_api_key_here
OPENAI_API_KEY=your_openai_api_key_here
LOG_LEVEL=INFO
```

### 4. Run the consumer

```bash
python main.py
```

### 5. Run the FastAPI server

```bash
uvicorn api.data_apis:app --reload
```

---

## API Reference

### `GET /semantic-search`

| Parameter | Required | Description |
|-----------|----------|-------------|
| `query_text` | Yes | Free-text query to search |

Returns top-5 Pinecone matches from the `week3` index.

---

### `GET /ask-knowledge`

| Parameter | Required | Description |
|-----------|----------|-------------|
| `query` | Yes | Question to answer (min length 1) |

Retrieves top-5 matches from the `infra-knowledge` index, feeds them as context to `gpt-4o-mini`, and returns a grounded answer.

**Example Response**
```json
{ "answer": "Kafka is a distributed event streaming platform used for..." }
```

---

## Key Concepts & Learnings

### Structured JSON logging with context propagation

All log output is JSON — every field is queryable in a log aggregator. `request_id` is stored in a `ContextVar` and automatically included in any log emitted during that request. INFO/DEBUG goes to stdout; WARNING+ goes to stderr, matching 12-factor app conventions.

### Latency tracking as a context manager

`track_latency(step, request_id)` wraps any block and emits a `step_latency` log line with `latency_ms` and `success` on exit — even on exception. This gives per-step timing (encode, upsert, search) without instrumenting every function individually.

### Retry with tenacity

Both Pinecone upserts and OpenAI calls use `@retry` with exponential backoff. Pinecone retries on 5xx `PineconeApiException`; OpenAI retries on `APIConnectionError`. This makes transient failures invisible to the caller without manual try/except retry loops.

### Multi-topic consumer with threading

`TOPIC_CONFIGS` in `.env` declares which topics to consume (`topic:group_id` pairs). One daemon thread per topic is spawned — each runs its own `poll()` loop. Topic handlers are registered in a `_HANDLERS` dict, making it trivial to add a new topic without touching consumer wiring.

### Clean architecture separation

The codebase is split into `core/` (cross-cutting concerns), `db/` (Pinecone I/O), `llm/` (OpenAI), `model/` (Pydantic schemas), `service/` (orchestration), and `api/` (HTTP). No layer imports from a layer above it — `db/` never imports `service/`, etc.

### Token-aware embedding with truncation

Before encoding, `_safe_encode` checks token count against `MAX_TOKENS=512`. If the message exceeds the limit, it is truncated to exactly 512 tokens using the tokenizer's own decode — avoiding silent silent embedding degradation on long inputs.
