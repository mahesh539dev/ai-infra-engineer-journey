# Python: RAG Production Service

Hardened Kafka consumer + FastAPI RAG endpoint — structured logging, latency tracking, retry, multi-topic threading, and OpenAI-powered answer generation.

---

## What This Does

- Runs one Kafka consumer thread per topic (configured via `TOPIC_CONFIGS`)
- Validates messages into typed Pydantic models at the deserializer boundary
- Token-checks and encodes text to 384-dim vectors with `all-MiniLM-L6-v2`
- Upserts to Pinecone with tenacity retry on 5xx errors
- Exposes `GET /semantic-search` and `GET /ask-knowledge` via FastAPI
- Answers questions by retrieving Pinecone context and calling OpenAI `gpt-4o-mini`
- Emits structured JSON logs to stdout/stderr with per-step latency tracking

---

## Package Structure

```
python/
├── main.py                       # entrypoint
├── api/
│   └── data_apis.py              # FastAPI endpoints
├── core/
│   ├── context.py                # ContextVar request_id
│   ├── logging_config.py         # JSON structured logging
│   └── measure_latency.py        # track_latency() context manager
├── db/
│   ├── pinecone_setup.py         # Pinecone client + index cache
│   ├── pinecone_insertion.py     # batch upsert with retry
│   └── pinecone_retrieval.py     # semantic search top-k
├── llm/
│   ├── rag_answer.py             # OpenAI RAG answer generation
│   └── async_call.py             # standalone interactive LLM script
├── model/
│   ├── learning_ai_event.py      # schema for learning-ai topic
│   └── rag_event.py              # schema for rag-events topic
├── service/
│   ├── embedding_service.py      # encode + upsert + search
│   └── kafka_consumer_service.py # multi-topic threaded consumer
└── tests/
    ├── test_rag_answer.py
    └── test_ask_endpoint.py
```

---

## Setup

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

---

## Running

### Consumer (Kafka → Pinecone ingestion)

```bash
python main.py
```

Starts one thread per topic defined in `TOPIC_CONFIGS`. Sample output:
```json
{"timestamp": "...", "level": "INFO", "message": "Consumer started: topic=learning-ai group=learning-ai-cg1"}
{"timestamp": "...", "level": "INFO", "message": "step_latency", "step": "upsert_data", "latency_ms": 42.3, "success": true}
```

### FastAPI server

```bash
uvicorn api.data_apis:app --reload
```

### Tests

```bash
pytest tests/
```

---

## Key Concepts & Learnings

### Structured JSON logging

`JsonFormatter` serializes every log record as JSON, including `request_id` from a `ContextVar`, file, function, and line number. INFO/DEBUG routes to stdout; WARNING+ to stderr — compatible with any log aggregator.

### Latency tracking as a context manager

```python
with track_latency("encode_embeddings", request_id=None, record_count=len(eventList)):
    embeddings = self._encode_batch(...)
```

Emits a `step_latency` log line with `latency_ms` and `success` on exit, even on exception. No manual timing code needed at each call site.

### Retry with tenacity

```python
@retry(stop=stop_after_attempt(3), wait=wait_exponential(min=1, max=5),
       retry=retry_if_exception(lambda e: isinstance(e, PineconeApiException) and e.status >= 500))
def upsert(...):
```

Pinecone upserts retry on 5xx; OpenAI retries on `APIConnectionError`. Exponential backoff (1–5s) avoids hammering on transient failures.

### Multi-topic threaded consumer

`TOPIC_CONFIGS=topic:group_id,topic:group_id` in `.env` drives `run_all_consumers()` — one daemon thread per topic, each with its own `poll()` loop. Topic handlers are registered in a `_HANDLERS` dict, so adding a new topic is a two-line change.

### Token-aware encoding

Before calling `SentenceTransformer.encode()`, `_safe_encode` counts tokens and truncates to `MAX_TOKENS=512` via the tokenizer's own decode — preventing silent embedding degradation on long inputs.

### RAG answer generation

`generate_answer()` builds a context string from Pinecone result metadata and sends it to `gpt-4o-mini` with `temperature=0.3`. A strict system prompt constrains the model to only use the provided context — reducing hallucination in a grounded retrieval scenario.
