# Week 3: Real-Time AI Event Pipeline

> Kafka → Vector Embeddings → Semantic Search

A full end-to-end AI infrastructure pipeline built across two services: a Spring Boot application that produces Kafka events and serves a reactive search API, and a Python consumer that transforms those events into vector embeddings stored in Pinecone.

---

## System Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                        Docker (Infrastructure)                  │
│   ┌──────────────────┐          ┌──────────────────────────┐    │
│   │   Kafka Broker   │          │        Kafka UI           │    │
│   │   (port 9092)    │          │       (port 8080)         │    │
│   └────────┬─────────┘          └──────────────────────────┘    │
└────────────┼────────────────────────────────────────────────────┘
             │
             │  topic: learning-ai
             │
┌────────────▼────────────┐          ┌──────────────────────────┐
│   Spring Boot Service   │          │     Python Consumer       │
│   (port 8090)           │          │                          │
│                         │          │  kafka-consumer.py        │
│  Spring Cloud Stream    │─produces─►  ↓ Pydantic validation   │
│  (Kafka producer)       │          │  ↓ SentenceTransformer   │
│                         │          │    (384-dim embeddings)   │
│  WebFlux REST API       │          │  ↓ pinecone_insertion.py │
│  GET /retrieve?topic=X  │◄─────────│                          │
│  (calls search service) │          │  Pinecone (serverless)   │
│                         │  HTTP    │  index: week3            │
│  WebClient downstream   │─────────►│  metric: cosine          │
└─────────────────────────┘          └──────────────────────────┘
```

---

## Data Flow

1. Spring Boot publishes a `KafkaEvent` (message, topic, subtopic) to the `learning-ai` Kafka topic via Spring Cloud Stream
2. Python consumer polls Kafka in batches of up to 100 messages
3. Each message is validated into a `KafkaEvent` Pydantic model at the deserializer boundary
4. `SentenceTransformer` (`all-MiniLM-L6-v2`) encodes the message text into a 384-dimensional vector
5. Vectors are batch-upserted into Pinecone with ID `{topic}-{offset}` and metadata (topic, subtopic)
6. Spring Boot exposes `GET /retrieve?topic=X` — a reactive gateway that calls the downstream search service and returns ranked results

---

## Component Map

| Component | Language | Role | README |
|-----------|----------|------|--------|
| `spring/kafka.integration` | Java 17 + Spring Boot 4 | Kafka producer + reactive search gateway | [README](spring/kafka.integration/README.md) |
| `python/` | Python 3 | Kafka consumer + Pinecone vector insertion | [README](python/README.md) |
| `docker-compose.yml` | Docker | Kafka broker + Kafka UI | this file |

---

## Infrastructure Setup

Start Kafka and the Kafka UI:

```bash
docker compose up -d
```

| Service | URL |
|---------|-----|
| Kafka broker | `localhost:9092` |
| Kafka UI | `http://localhost:8080` |

---

## Key Technologies

| Technology | Purpose |
|------------|---------|
| Apache Kafka | Async event streaming between services |
| Spring Cloud Stream | Kafka producer abstraction for Spring |
| Spring WebFlux | Non-blocking reactive REST API |
| Pydantic | Runtime type validation for Kafka messages |
| SentenceTransformers | Text → dense vector embeddings |
| Pinecone | Managed vector database for semantic search |
