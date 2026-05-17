# Spring Boot: RAG PDF Ingestion Pipeline

A Spring Boot 4 application that watches a folder for PDFs, chunks them with a recursive character text splitter, and publishes each chunk as a structured `ChunkEvent` to Kafka — the ingestion side of a RAG pipeline.

---

## What This Does

- Scans `./files` on startup and processes any existing PDFs immediately
- Watches the same folder for new PDFs dropped at runtime (Java `WatchService`)
- Extracts text from each PDF using PDFBox with configurable retry
- Splits text using `RecursiveCharacterTextSplitter` (paragraph → line → word → character, with overlap)
- Publishes each chunk as a `ChunkEvent` to the `rag-events` Kafka topic via Spring Cloud Stream
- Moves processed PDFs to `./files/processed/` and failed ones to `./files/error/`
- Also exposes `GET /retrieve?topic=X` — a reactive WebFlux endpoint that proxies searches to a downstream vector service

---

## Architecture

```
FileWatcherService (@PostConstruct + WatchService thread)
    │
    ▼
PdfProcessingService
    ├── PDFBox text extraction (retry: maxAttempts=3, delaySeconds=5)
    ├── RecursiveCharacterTextSplitter (chunkSize=1000, chunkOverlap=400)
    │     separators: ["\n\n", "\n", " ", ""]
    └── ChunkEvent builder
          document: {id, name, type, sourcePath}
          chunk:    {id, index, totalChunks, text, characterCount}
          processing: {strategy, chunkSize, chunkOverlap}
          pinecone: {indexName, namespace}
          metadata: {tags, language}
    │
    ▼
KafkaProducer (StreamBridge)
    ├── "rag-producer"  → topic: rag-events   (ChunkEvent)
    └── "ai-producer"   → topic: learning-ai  (KafkaEvent)
```

---

## Key Files

| File | Responsibility |
|------|----------------|
| `RAGApplication.java` | Spring Boot entry point |
| `FileWatcherService.java` | `@PostConstruct` startup scan + `WatchService` hot-drop watcher |
| `PdfProcessingService.java` | PDF extraction, retry, chunking, event building, file move |
| `RecursiveCharacterTextSplitter.java` | Recursive separator-aware text chunker with overlap |
| `ChunkEvent.java` | Immutable record — full chunk event schema with nested records |
| `KafkaProducer.java` | StreamBridge wrapper for both `rag-producer` and `ai-producer` bindings |
| `FileWatcherProperties.java` | `@ConfigurationProperties` — type-safe config record |
| `FileWatcherConfig.java` | `@EnableConfigurationProperties` registration |

---

## Configuration (`application.yaml`)

```yaml
file:
  watcher:
    directory: ./files
    chunk-size: 1000
    chunk-overlap: 400
    retry:
      max-attempts: 3
      delay-seconds: 5
    pinecone:
      index-name: infra-knowledge
      namespace: file-data
    metadata:
      language: en
      tags: ["knowledge", "learning"]

spring:
  cloud:
    stream:
      bindings:
        rag-producer:
          destination: rag-events
        ai-producer:
          destination: learning-ai
      kafka:
        binder:
          brokers: localhost:9092

server:
  port: 8090
```

---

## Setup & Run

**Prerequisites:** Java 17+, Maven 3.8+, Kafka on `localhost:9092` (`docker compose up -d` from `week3/`)

```bash
./mvnw spring-boot:run
```

Drop a PDF into `./files/` — the watcher picks it up, chunks it, and publishes to `rag-events`.

---

## Tests

| Test class | What it covers |
|------------|---------------|
| `RecursiveCharacterTextSplitterTest` | null/blank input, single chunk, all chunks within size, paragraph separator priority, character-level overlap |
| `KafkaProducerChunkTest` | `sendChunkEvent` routes to `rag-producer` binding |
| `ChunkEventTest` | Record construction and field access |
| `FileWatcherServiceTest` | `@PostConstruct` startup behaviour |
| `PdfProcessingServiceTest` | Chunk building and Kafka publish per chunk |

---

## Key Implementation Details

### RecursiveCharacterTextSplitter
Mirrors LangChain's `RecursiveCharacterTextSplitter`. Tries separators in order (`\n\n` → `\n` → ` ` → `""`), recursing into sub-splits when a segment still exceeds `chunkSize`. `mergeInto` accumulates parts back up to `chunkSize`, then slides the window back by `chunkOverlap` to preserve context across boundaries.

### ChunkEvent schema
Each event carries full provenance: `trace_id` and `request_id` (UUID per document) correlate all chunks from the same PDF. `chunk_index` and `total_chunks` allow consumers to detect incomplete ingestion. The `pinecone` sub-record tells the consumer exactly which index and namespace to target — no consumer-side config required.

### FileWatcherService lifecycle
`@PostConstruct` processes existing PDFs synchronously before the watcher thread starts — guaranteeing no PDF in the drop folder is silently skipped on restart. `@PreDestroy` shuts down the executor and closes the `WatchService` cleanly.

### StreamBridge bindings
`KafkaProducer` uses two named bindings: `rag-producer` for `ChunkEvent` (RAG pipeline) and `ai-producer` for `KafkaEvent` (legacy topic). Each binding maps to a separate Kafka topic with `useNativeEncoding: true` and `JsonSerializer`.
