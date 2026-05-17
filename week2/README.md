# Week 2 — Embeddings & Semantic Search: ChromaDB, Pinecone, pgvector

> **Part of my AI Infrastructure Engineer learning journey.**
> Senior Java / Kafka / Spring Boot engineer (7 years) building AI skills one project per week. See the [main README](../README.md) for the full roadmap and progress tracking.

---

## What This Project Is

A **full-stack semantic search system** — five days of Python vector database work culminating in a production-grade capstone: a **Spring Boot 4 (WebFlux) API** that proxies semantic search queries to a **FastAPI + ChromaDB** Python backend, with PDF chunking, reactive error handling, integration tests, and Swagger UI.

This is Week 2. The goal was to go deep on how vector databases actually work — not just use one, but implement the same semantic search pipeline across three architecturally different systems, benchmark them, and then build a real Java ↔ Python integration that mirrors how AI gets wired into enterprise stacks.

---

## Project Structure

```
week2/
├── day1_embeddings_fundamentals.py       # Cosine similarity matrix, PCA visualization
├── day2_chromadb_semantic_search.py      # ChromaDB CRUD, semantic search, metadata filters
├── day3_pinecone_production_scale.py     # Pinecone serverless, batch upsert, metadata delete
├── day4_pgvector_hybrid_search.py        # pgvector HNSW, hybrid keyword+semantic SQL
├── day5_multi_vectordb_comparison.py     # Benchmark all 3 DBs, comparison table, JSON report
├── sample_documents.json                 # 30 test documents (AI, databases, programming, NLP)
├── vectordb_comparison_report.json       # Generated benchmark output
├── docker-compose.yml                    # PostgreSQL + pgvector (port 5434)
└── capstone-project/
    ├── datasearchapi.py                  # FastAPI app — GET /search-semantic
    ├── chroma_actions.py                 # ChromaDB client, load_data_to_chroma, query_data
    ├── fileDataReader.py                 # PDF → page text → overlapping chunks
    ├── main.py                           # Uvicorn entrypoint
    ├── capstone_test_document.pdf        # Test PDF loaded into ChromaDB on startup
    └── semanticsearch/                   # Spring Boot 4 (WebFlux) Java service
        ├── pom.xml                       # Spring Boot 4.0.6, WebFlux, SpringDoc OpenAPI
        └── src/
            ├── main/
            │   ├── resources/application.yaml
            │   └── java/.../semanticsearch/
            │       ├── config/WebClientConfig.java     # WebClient wired to FastAPI base-url
            │       ├── router/SearchRouter.java        # Functional route: GET /api/search
            │       ├── handler/SearchHandler.java      # Input validation, delegates to service
            │       ├── service/SearchService.java      # Reactive WebClient call to FastAPI
            │       ├── model/                          # SearchResponse, ResultEntry, Metadata records
            │       └── exception/                      # GlobalErrorHandler, typed exceptions
            └── test/
                └── SearchIntegrationTest.java          # 8 integration tests with MockWebServer
```

---

## Capstone: Spring Boot → FastAPI Semantic Search

The capstone wires together everything from the week into a two-service system that demonstrates how Java enterprise APIs delegate AI workloads to Python.

### Architecture

```
Client (curl / browser / Postman)
        │
        │  GET /api/search?query_text=...&num_results=5
        ▼
┌─────────────────────────────────────────────┐
│  Spring Boot 4 (WebFlux)  :8080             │
│                                             │
│  SearchRouter  →  SearchHandler             │
│      validates query_text (blank check)     │
│      validates num_results (int, min 1)     │
│           │                                 │
│  SearchService (reactive WebClient)         │
│      GET http://localhost:8000/search-      │
│           semantic?query_text=...           │
│      maps 4xx/5xx → DownstreamException     │
│      maps connection fail → ServiceUnavail  │
│           │                                 │
│  GlobalErrorHandler                         │
│      400 ValidationException                │
│      502 DownstreamException                │
│      503 ServiceUnavailableException        │
└─────────────────────────────────────────────┘
        │
        │  GET /search-semantic?query_text=...&num_results=5
        ▼
┌─────────────────────────────────────────────┐
│  FastAPI (Python)  :8000                    │
│                                             │
│  lifespan startup:                          │
│    fileDataReader → extract PDF pages       │
│    chunk_text (1000 chars, 100 overlap)     │
│    ChromaDB.collection.add(chunks)          │
│                                             │
│  GET /search-semantic                       │
│    collection.query(query_texts=[...])      │
│    similarity = 1 - cosine_distance         │
│    returns ranked results with metadata     │
└─────────────────────────────────────────────┘
        │
        ▼
┌─────────────────────────────────────────────┐
│  ChromaDB (persistent)                      │
│  Collection: capstone-week2                 │
│  HNSW cosine, ef_construction=200           │
│  Documents: PDF chunks (page + chunk idx)  │
└─────────────────────────────────────────────┘
```

### API Endpoints

**Spring Boot — port 8080**

| Method | Endpoint | Description |
|--------|----------|-------------|
| `GET` | `/api/search?query_text=...&num_results=5` | Semantic search proxy — validates input, calls FastAPI, returns ranked results |

**FastAPI — port 8000**

| Method | Endpoint | Description |
|--------|----------|-------------|
| `GET` | `/search-semantic?query_text=...&num_results=5` | Queries ChromaDB collection, returns similarity-ranked PDF chunks |

### Example Request & Response

```bash
curl "http://localhost:8080/api/search?query_text=machine+learning&num_results=3"
```

```json
{
  "results": {
    "result_1": {
      "text": "Machine learning models learn patterns from training data...",
      "metadata": { "page": 2, "chunk": 0 },
      "similarity": 0.8741
    },
    "result_2": {
      "text": "Neural networks are a class of machine learning algorithms...",
      "metadata": { "page": 3, "chunk": 1 },
      "similarity": 0.8123
    },
    "result_3": {
      "text": "Supervised learning requires labeled datasets...",
      "metadata": { "page": 2, "chunk": 2 },
      "similarity": 0.7894
    }
  }
}
```

### Error Handling

| Scenario | HTTP Status | Response |
|----------|-------------|----------|
| Missing or blank `query_text` | `400` | `{"status":400,"error":"Bad Request","message":"query_text must not be blank"}` |
| `num_results` not an integer | `400` | `{"status":400,"error":"Bad Request","message":"num_results must be a valid integer"}` |
| `num_results` < 1 | `400` | `{"status":400,"error":"Bad Request","message":"num_results must be at least 1"}` |
| FastAPI returns 4xx or 5xx | `502` | `{"status":502,"error":"Bad Gateway","message":"Downstream error: 500"}` |
| FastAPI unreachable | `503` | `{"status":503,"error":"Service Unavailable","message":"Search service is unavailable"}` |

---

## Daily Exercises (Days 1–5)

### Day 1 — Embeddings Fundamentals
- Encoded 6 documents with `all-MiniLM-L6-v2` → 384-dim vectors
- Built full cosine similarity matrix using `sklearn.metrics.pairwise.cosine_similarity`
- Reduced to 2D with PCA → saved `embeddings_visualization.png`
- Verified embedding norms ≈ 1.0 (unit vectors — cosine similarity = dot product)

### Day 2 — ChromaDB Semantic Search
- `PersistentClient` pointing to `./chroma_data` — survives process restarts
- Collection with `hnsw:space: cosine` for cosine similarity
- CRUD: add, query, update (doc_001), delete (doc_006)
- Filtered search using `where={"topic": "ai"}` metadata predicate

### Day 3 — Pinecone Production Scale
- Serverless index on AWS `us-east-1`, dimension=384, cosine metric
- Manual embedding: `model.encode(text).tolist()` → upserted as `(id, vector, metadata)`
- Batch upsert in groups of 100 (production best practice)
- Metadata filter: `filter={"topic": {"$eq": "ai"}}`
- Delete by metadata: `filter={"length": {"$gt": 60}}`

### Day 4 — pgvector Hybrid Search
- PostgreSQL with pgvector extension via Docker on port 5434
- HNSW index: `USING hnsw (embedding vector_cosine_ops) WITH (m=5, ef_construction=200)`
- `<=>` operator = cosine distance; similarity = `1 - (embedding <=> query::vector)`
- Hybrid search: SQL `WHERE topic = %s` filter combined with vector `ORDER BY`
- `execute_values` for efficient batch inserts; `%s` parametrized queries throughout

### Day 5 — Multi-DB Comparison Benchmark
- Ran identical insert + query workload against ChromaDB, Pinecone, and pgvector
- Collected per-query latencies, computed averages, rendered comparison table with `tabulate`

| Database | Insert Time | Avg Query Latency | Best For |
|----------|-------------|-------------------|----------|
| ChromaDB | 1.93s | 470.96ms | Local development, prototyping |
| Pinecone | 1.29s | 59.04ms | Production SaaS, cloud scale |
| pgvector | **0.68s** | **7.96ms** | Existing PostgreSQL stack |

> pgvector won on both insert speed and query latency at this dataset size. ChromaDB's latency reflects cold-start model loading in the test harness — a persistent server would be far faster.

---

## Tech Stack

| Layer | Technology | Notes |
|-------|-----------|-------|
| Java API | Spring Boot 4 (WebFlux) | Reactive, functional router pattern |
| HTTP Client | Spring WebClient | Non-blocking call to FastAPI |
| API Docs | SpringDoc OpenAPI (WebFlux) | Swagger UI at `/swagger-ui.html` |
| Python API | FastAPI + Uvicorn | Lifespan startup for PDF ingestion |
| Vector DB | ChromaDB (persistent) | PDF chunks, cosine HNSW, auto-embeds |
| Embeddings | SentenceTransformers | `all-MiniLM-L6-v2`, 384-dim |
| PDF Parsing | pypdf | Page extraction + overlapping chunking |
| Cloud Vector DB | Pinecone | Serverless, manual embeddings |
| SQL Vector DB | pgvector + PostgreSQL | HNSW, hybrid search, `<=>` operator |
| Infrastructure | Docker Compose | PostgreSQL + pgvector on port 5434 |
| Testing | JUnit 5 + MockWebServer | 8 integration tests, no real HTTP needed |

---

## Setup & Run

### 1. Start PostgreSQL (for Day 4 / Day 5)

```bash
cd week2
docker-compose up -d
```

### 2. Run the Python daily exercises

```bash
python -m venv venv
venv\Scripts\activate   # Windows

pip install chromadb pinecone-client psycopg2-binary pgvector \
            sentence-transformers tabulate python-dotenv scikit-learn matplotlib

# Add Pinecone API key
echo PINECONE_API_KEY=your_key_here > .env

python day1_embeddings_fundamentals.py
python day2_chromadb_semantic_search.py
python day3_pinecone_production_scale.py
python day4_pgvector_hybrid_search.py
python day5_multi_vectordb_comparison.py
```

### 3. Start the FastAPI semantic search service

```bash
cd week2/capstone-project

pip install fastapi uvicorn chromadb sentence-transformers pypdf python-dotenv

# Runs on :8000
# On startup: loads PDF → chunks → embeds → stores in ChromaDB
python main.py
```

FastAPI docs available at `http://localhost:8000/docs`

### 4. Start the Spring Boot proxy

```bash
cd week2/capstone-project/semanticsearch

./mvnw spring-boot:run
# Runs on :8080
```

Swagger UI available at `http://localhost:8080/swagger-ui.html`

### 5. Query end-to-end

```bash
curl "http://localhost:8080/api/search?query_text=machine+learning&num_results=3"
```

---

## Key Implementation Details

### FastAPI — PDF Chunking Pipeline
- `fileDataReader.py` reads each PDF page with `pypdf`, then splits into **1000-char chunks with 100-char overlap** — the overlap prevents semantic context from being cut across chunk boundaries
- Chunk IDs follow `page{N}_chunk{M}` format, metadata carries `page` and `chunk` index for traceability back to the source document
- ChromaDB auto-embeds on `collection.add()` using its default `all-MiniLM-L6-v2` model — no manual encoding needed

### Spring Boot — Reactive WebClient
- `WebClientConfig` reads `search.api.base-url` from `application.yaml` → injects a `WebClient` bean
- `SearchService.search()` builds the URI with `uriBuilder` to safely encode query parameters (no manual string concatenation)
- `.onStatus()` maps any 4xx/5xx from FastAPI to `DownstreamException` → rendered as `502` by `GlobalErrorHandler`
- `.onErrorMap(WebClientRequestException.class, ...)` catches connection refused / timeout → rendered as `503`

### Spring Boot — Functional Router Pattern
- Uses `RouterFunctions.route()` + `SearchHandler` instead of `@RestController` — the WebFlux functional style, closer to how Spring Cloud Gateway and reactive microservices are structured
- Full `@RouterOperation` OpenAPI annotations on the `@Bean` method to generate Swagger docs without `@RestController`

### Testing — MockWebServer
- `SearchIntegrationTest` spins up a real Spring Boot context on a random port and a `MockWebServer` in place of FastAPI
- `@DynamicPropertySource` rewires `search.api.base-url` to the mock server URL at test time — no mocking of Spring internals, tests the full reactive pipeline
- Covers: happy path, default `num_results`, blank query, non-integer `num_results`, `num_results < 1`, 502 from downstream 500, 502 from downstream 404

---

## What I Learned This Week

**Coming from Java/Spring Boot**, the mental model shifts were:

| Java / Spring Boot | Python / FastAPI + Vector DBs |
|-------------------|-------------------------------|
| `@RestController` + `@GetMapping` | `RouterFunctions.route()` (functional WebFlux) AND `@app.get()` (FastAPI) — two styles, same concept |
| `@SpringBootApplication` + `ApplicationRunner` | FastAPI `lifespan` context manager — startup logic before first request |
| JPA `@Entity` + SQL schema | ChromaDB `collection.add()` — schema-free, embeddings auto-generated |
| JDBC `PreparedStatement` / `%s` | psycopg2 `%s` placeholders — identical concept, different syntax |
| ElasticSearch `bool` query + filter | ChromaDB/Pinecone `where` metadata predicate |
| Spring `WebClient` for HTTP | `requests` / `httpx` in Python — same reactive mental model, different runtime |
| `MockMvc` / `WebTestClient` + `WireMock` | `MockWebServer` (OkHttp) with `@DynamicPropertySource` |

**New concepts I genuinely hadn't used before:**

- **HNSW indexing** — Hierarchical Navigable Small World graph; approximate nearest neighbor that trades tiny accuracy loss for massive speed gain. Same algorithm used by ChromaDB, Pinecone, and pgvector — one mental model covers all three.
- **Cosine vs. L2 distance** — For normalized unit vectors, cosine similarity = dot product. The `<=>` operator in pgvector returns cosine *distance*; `1 - distance` gives similarity.
- **Vector column in SQL** — `embedding vector(384)` is a first-class PostgreSQL column type. You get HNSW indexing on it and can mix freely with standard SQL `WHERE` — hybrid search for free.
- **PDF chunking with overlap** — Splitting at fixed character boundaries loses context at the seam. 100-char overlap means each chunk shares context with its neighbours, so the most relevant passage is never accidentally split.
- **Serverless vector DB tradeoffs** — Pinecone's 59ms vs. pgvector's 8ms on 30 docs is almost entirely network round-trip. At 1M docs the trade reverses — pgvector needs more hardware, Pinecone scales automatically without ops work.

---

## Author

**Mahesh Annapureddy** — Senior Software Engineer → AI Infrastructure Engineer

Building in public. One project per week. All code on GitHub.
