# File Watcher — Design Spec
**Date:** 2026-05-11  
**Project:** kafka.integration (Week 3 — Spring Kafka)  
**Status:** Approved

---

## Goal

Watch a configurable directory for incoming PDF files, extract their text, split it into chunks using recursive character splitting, and publish each chunk as a structured `ChunkEvent` to a dedicated Kafka topic (`rag-topic`). After successful processing, move the file to a `processed/` subfolder. On unrecoverable extraction failure, move it to an `error/` subfolder.

This feeds a downstream RAG pipeline (Kafka → embedding → Pinecone).

---

## Architecture

Four new components are added to the existing project structure. No existing components are removed; `KafkaProducer` receives a small addition only.

### New Components

| Component | Package | Responsibility |
|---|---|---|
| `FileWatcherService` | `service` | Starts a Java NIO `WatchService` on a background thread; scans watch dir on startup for existing PDFs |
| `PdfProcessingService` | `service` | Orchestrates: PDF text extraction → chunking → event building → Kafka publish → file move |
| `RecursiveCharacterTextSplitter` | `util` | Pure utility — splits text on `\n\n → \n → space → char` until all chunks are within the configured size |
| `ChunkEvent` (+ nested records) | `model` | Immutable Java records matching the target Kafka JSON schema |

### Modified Components

| Component | Change |
|---|---|
| `KafkaProducer` | Add `sendChunkEvent(ChunkEvent)` method targeting the new `rag-producer` binding |

### New Dependency

```xml
<dependency>
    <groupId>org.apache.pdfbox</groupId>
    <artifactId>pdfbox</artifactId>
    <version>3.0.3</version>
</dependency>
```

---

## Data Flow

```
PDF dropped in watch directory
        │
        ▼
FileWatcherService
  ├── Startup scan: process any existing PDFs in watch dir
  └── WatchService loop: listen for ENTRY_CREATE events on *.pdf files
        │
        ▼
PdfProcessingService.process(Path filePath)
        │
        ├─ 1. Extract full text via Apache PDFBox
        │      └── On failure: retry up to N times with delay
        │                      → on exhaustion: move to error/, throw exception
        │
        ├─ 2. RecursiveCharacterTextSplitter.split(text, chunkSize, overlap)
        │      └── Returns List<String> chunks
        │
        ├─ 3. Build ChunkEvent per chunk
        │      ├── event_id:     UUID (per chunk)
        │      ├── trace_id:     UUID (shared across all chunks of this doc)
        │      ├── request_id:   UUID (per file processing run)
        │      ├── chunk_index:  1-based
        │      ├── total_chunks: chunks.size()
        │      └── all other fields: from file metadata + config
        │
        ├─ 4. KafkaProducer.sendChunkEvent(chunk) → rag-producer → rag-topic
        │      └── On failure: stop processing, leave file in place, log error
        │
        └─ 5. Move file to processed/ subfolder
               └── On failure: log warning only (chunks already published)
```

---

## ChunkEvent Model (Java Records)

```java
// Top-level record
record ChunkEvent(
    @JsonProperty("event_id")      String eventId,
    @JsonProperty("event_type")    String eventType,
    @JsonProperty("event_version") String eventVersion,
    @JsonProperty("timestamp")     String timestamp,
    @JsonProperty("request_id")    String requestId,
    @JsonProperty("trace_id")      String traceId,
    @JsonProperty("document")      Document document,
    @JsonProperty("chunk")         Chunk chunk,
    @JsonProperty("processing")    Processing processing,
    @JsonProperty("pinecone")      Pinecone pinecone,
    @JsonProperty("metadata")      Metadata metadata
)

record Document(
    @JsonProperty("document_id")   String documentId,
    @JsonProperty("document_name") String documentName,
    @JsonProperty("document_type") String documentType,
    @JsonProperty("source_path")   String sourcePath
)

record Chunk(
    @JsonProperty("chunk_id")       String chunkId,
    @JsonProperty("chunk_index")    int chunkIndex,
    @JsonProperty("total_chunks")   int totalChunks,
    @JsonProperty("text")           String text,
    @JsonProperty("character_count") int characterCount
)

record Processing(
    @JsonProperty("chunking_strategy") String chunkingStrategy,
    @JsonProperty("chunk_size")        int chunkSize,
    @JsonProperty("chunk_overlap")     int chunkOverlap
)

record Pinecone(
    @JsonProperty("index_name") String indexName,
    @JsonProperty("namespace")  String namespace
)

record Metadata(
    @JsonProperty("tags")     List<String> tags,
    @JsonProperty("language") String language
)
```

All records live in `com.ai.learning.kafka.integration.model`.

---

## Configuration (`application.yaml`)

```yaml
spring:
  cloud:
    stream:
      bindings:
        rag-producer:
          destination: rag-topic
          producer:
            useNativeEncoding: true

file:
  watcher:
    directory: ./watched-files
    chunk-size: 1000
    chunk-overlap: 200
    retry:
      max-attempts: 3
      delay-seconds: 5
    pinecone:
      index-name: infra-knowledge
      namespace: default
    metadata:
      language: en
      tags: []
```

Config is bound via a `@ConfigurationProperties` class `FileWatcherProperties`.

---

## Error Handling

| Failure Point | Behaviour |
|---|---|
| PDF extraction fails | Retry up to `retry.max-attempts` times, waiting `retry.delay-seconds` between each attempt |
| All retries exhausted | Move file to `error/` subfolder; log the exception; stop processing this file |
| Kafka publish fails (any chunk) | Stop processing remaining chunks; leave file in place (retried on next app startup); log error |
| Move to `processed/` fails | Log warning only — chunks are already published, no further action |
| Move to `error/` fails | Log warning only |

Directory layout inside the watch dir:
```
./watched-files/
├── incoming.pdf          ← dropped here
├── processed/
│   └── incoming.pdf      ← moved here on success
└── error/
    └── bad.pdf           ← moved here on unrecoverable failure
```

---

## Recursive Character Text Splitter

Splits text by trying separators in order: `["\n\n", "\n", " ", ""]`.

Algorithm:
1. Try splitting on the first separator. If any resulting piece exceeds `chunkSize`, recurse on that piece with the next separator.
2. Merge pieces greedily into chunks up to `chunkSize`, carrying forward `chunkOverlap` characters from the previous chunk.
3. Result: all chunks are ≤ `chunkSize` characters; consecutive chunks share `chunkOverlap` characters.

Config defaults: `chunkSize=1000`, `chunkOverlap=200` (range: size 800–1200, overlap 150–250).

---

## Testing

| Test Class | What it covers |
|---|---|
| `RecursiveCharacterTextSplitterTest` | Chunk sizes within bounds; overlap correctness; empty input; single-word input |
| `PdfProcessingServiceTest` | Correct chunk count; correct field mapping in `ChunkEvent`; retry logic on extraction failure; file moved to `error/` after exhausted retries; Kafka failure leaves file in place |
| `FileWatcherServiceTest` | Startup scan picks up existing PDFs; new file creation triggers processing |