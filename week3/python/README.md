# Python: Kafka Consumer + Pinecone Vector Insertion

Consumes events from a Kafka topic, generates semantic embeddings, and upserts them into a Pinecone vector index for similarity search.

---

## What This Does

- Polls the `learning-ai` Kafka topic in batches
- Validates each message into a typed `KafkaEvent` model using Pydantic
- Encodes the event's message text into a 384-dimensional vector using `SentenceTransformer`
- Batch-upserts vectors into Pinecone with topic/subtopic metadata

---

## Architecture

```
kafka-consumer.py
    │
    ├── KafkaConsumer (kafka-python)
    │     └── value_deserializer → KafkaEvent.model_validate_json()
    │                               (learning_ai_event.py)
    │
    └── upsert_data(events, offsets)
              (pinecone_insertion.py)
                    │
                    ├── SentenceTransformer.encode(message)
                    └── Pinecone.index.upsert(vectors)
```

### Files

| File | Responsibility |
|------|----------------|
| `learning_ai_event.py` | Pydantic model — schema and validation for Kafka messages |
| `kafka-consumer.py` | Kafka consumer loop — polls, validates, dispatches batches |
| `pinecone_insertion.py` | Pinecone client — encodes text to vectors and upserts |

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
pip install kafka-python pydantic sentence-transformers pinecone-client python-dotenv
```

### 3. Configure environment variables

Create a `.env` file in the `python/` directory:

```env
TOPIC_DATA=learning-ai
BROKERS=localhost:9092
PINECONE_API_KEY=your_pinecone_api_key_here
```

---

## Running

Make sure Kafka is running (`docker compose up -d` from the `week3/` root), then:

```bash
python kafka-consumer.py
```

The consumer will start polling and print progress:

```
✅ Index is ready
Consumer started. Waiting for messages...
Processing batch of 5 messages...
Inserted Data Successfully
```

---

## Key Concepts & Learnings

### Pydantic validation at the deserializer boundary

Instead of validating messages inside the consumer loop, validation is pushed into the Kafka `value_deserializer`. This means any malformed message fails immediately at ingestion — the rest of the pipeline only ever sees a valid, typed `KafkaEvent`. This is the earliest possible point to enforce schema correctness.

```python
value_deserializer=lambda x: KafkaEvent.model_validate_json(x.decode('utf-8'))
```

### Offset-based vector IDs

Pinecone requires a unique string ID per vector. Using `{topic}-{offset}` ties the vector identity to the Kafka partition offset — this is deterministic and idempotent (re-processing the same message produces the same ID, which upserts in place rather than creating duplicates).

### Batch processing with `consumer.poll()`

`poll()` returns a dict of `TopicPartition → [messages]`. The nested list comprehension flattens partition groups into a single list while preserving each message's offset alongside its value:

```python
messages_flat = [(msg.value, msg.offset) for messages in batch.values() for msg in messages]
```

### SentenceTransformer embeddings

`all-MiniLM-L6-v2` produces 384-dimensional dense vectors optimized for semantic similarity. It runs locally (no API call), is fast for batch workloads, and the 384-dim size keeps Pinecone storage costs low while maintaining good retrieval quality.

### Pinecone serverless index

The index is created on first run with `cosine` similarity metric. The `pc.has_index()` guard and the `while not ready` poll ensure the index exists and is ready before the consumer starts — avoiding a race condition at startup.
