# Week 1 — Python for AI: Data Pipeline & REST API

> **Part of my 90-Day AI Infrastructure Engineer journey.**
> I'm a Senior Java / Kafka / Spring Boot engineer (7 years) transitioning into AI Infrastructure. Each week I build a project that adds a new AI skill on top of my existing backend expertise.

---

## What This Project Is

A **data pipeline and REST API** built with Python, FastAPI, Pandas, and SentenceTransformers — using the Titanic dataset as the data source.

This is my first week of Python AI work. The goal was to move fast on things I already understand from Java (REST APIs, data pipelines, OOP) and go deep on what's genuinely new: NumPy/Pandas data processing, and my first hands-on experience with **embeddings and semantic similarity search**.

---

## API Endpoints

| Method | Endpoint | Description |
|--------|----------|-------------|
| `GET` | `/stats` | Full analysis: survival rate, age stats, gender & class breakdowns |
| `GET` | `/survival-rate` | Overall survival rate percentage |
| `GET` | `/by-gender` | Survival count split by gender |
| `GET` | `/similar-name?name=...` | Top 3 semantically similar passenger names using embeddings |

### Example Responses

**`GET /stats`**
```json
{
  "survival_rate_percentage": 38.38,
  "total_passengers": 889,
  "avg_survival_age": 28.34,
  "avg_nonsurvival_age": 30.63,
  "survival_count_by_gender": { "female": 231, "male": 109 },
  "survival_count_by_passenger_class": { "1": 136, "2": 87, "3": 117 }
}
```

**`GET /similar-name?name=John Smith`**
```json
[
  { "name": "Smith, Mr. James Clinch", "similarity": 0.812 },
  { "name": "Smith, Mr. Lucien Philip", "similarity": 0.798 },
  { "name": "Johnston, Mr. Andrew G", "similarity": 0.743 }
]
```

---

## Architecture

```
Titanic-Dataset.csv
        │
        ▼
  pipeline.py  ←── Pandas: clean nulls, drop Cabin column, compute stats
        │
        ▼
   stats.json  ←── persisted analysis results
        │
        ▼
   main.py (FastAPI)
        │
        ├── /stats         ←── reads stats.json
        ├── /survival-rate ←── reads stats.json
        ├── /by-gender     ←── reads stats.json
        └── /similar-name  ←── SentenceTransformer embeddings (cosine similarity)
                                pre-computed at startup via lifespan context
```

**Startup flow:** FastAPI's `lifespan` context manager runs `generate_clean_data()`, then pre-computes embeddings for all 889 passenger names using `all-MiniLM-L6-v2` — so every `/similar-name` query is instant (no re-encoding on each request).

---

## Key Implementation Details

### Data Cleaning (pipeline.py)
- Missing `Age` values filled with **median** (not mean — more robust to outliers)
- Rows with missing `Embarked` dropped (only 2 rows)
- `Cabin` column dropped (>77% missing — not recoverable)
- GroupBy results explicitly cast to native Python `int` for JSON serialization compatibility

### Semantic Name Search (main.py)
- Model: `sentence-transformers/all-MiniLM-L6-v2` — 384-dimensional embeddings, fast and accurate
- All 889 name embeddings pre-computed at startup → stored as a NumPy matrix
- Query embedding reshaped to `(1, 384)` for `cosine_similarity()` broadcasting
- `np.argsort()[-3:][::-1]` to get top 3 indices in descending similarity order

### FastAPI Lifespan Pattern
```python
@asynccontextmanager
async def lifespan(_: FastAPI):
    global model, names, name_embeddings
    generate_clean_data()                            # run pipeline once at startup
    model = SentenceTransformer("all-MiniLM-L6-v2")
    names = csv_data["Name"].tolist()
    name_embeddings = model.encode(names)            # pre-compute all embeddings
    yield
```
This replaces the deprecated `@app.on_event("startup")` — the modern FastAPI way to manage application lifecycle resources.

---

## Tech Stack

| Layer | Technology | Notes |
|-------|-----------|-------|
| API Framework | FastAPI | Python's Spring Boot equivalent |
| Data Processing | Pandas + NumPy | DataFrame cleaning, vectorized math |
| Embeddings | SentenceTransformers | `all-MiniLM-L6-v2`, 384-dim vectors |
| Similarity | scikit-learn cosine_similarity | Matrix-level computation |
| Runtime | Python 3.x + Uvicorn | ASGI server |

---

## Setup & Run

```bash
# Clone and create virtual environment
git clone <repo-url>
cd data-pipeline-api-week1
python -m venv venv

# Activate (Windows)
venv\Scripts\activate

# Install dependencies
pip install fastapi uvicorn pandas numpy scikit-learn sentence-transformers

# Run
uvicorn main:app --reload
```

Open http://localhost:8000/docs for the interactive Swagger UI.

> Note: First startup takes ~10–20 seconds while the embedding model downloads and encodes all passenger names. Subsequent requests to `/similar-name` are instant.

---

## What I Learned This Week

**Coming from Java/Spring Boot**, the mental model shifts were:

| Java / Spring Boot | Python / FastAPI |
|-------------------|-----------------|
| `@SpringBootApplication` + `ApplicationRunner` | `lifespan` context manager |
| `@RestController` + `@GetMapping` | `@app.get("/route")` |
| `ResponseEntity<T>` | Return dict directly — FastAPI serializes it |
| JDBC / JPA for data | Pandas DataFrames |
| Manual null checks | `fillna()`, `dropna()` — declarative |

**New concepts I genuinely hadn't used before:**
- **Embeddings** — turning text into dense vectors where semantic similarity = cosine proximity
- **Pre-computation pattern** — encode once at startup, query instantly at runtime (critical for production)
- **NumPy broadcasting** — why `reshape(1, -1)` is needed when computing similarity of one vector against a matrix
- **Pandas groupby serialization** — `.groupby().size()` returns an Index, not a plain dict; need explicit casting for JSON

---

## Roadmap Context

This is **Week 1 of 12** in my AI Infrastructure Engineer transition:

```
Phase 1 (Weeks 1–4): AI Foundations + Data Pipelines
  ✅ Week 1: Python for AI — NumPy, Pandas, FastAPI, Embeddings   ← YOU ARE HERE
  ⬜ Week 2: Embeddings & Semantic Search (ChromaDB, Pinecone, pgvector)
  ⬜ Week 3: AI Data Pipeline with Kafka
  ⬜ Week 4: Spring AI + ML Fundamentals

Phase 2 (Weeks 5–9): LLM Applications & RAG Systems
  ⬜ Week 5: Transformers, LLM APIs & Prompt Engineering
  ⬜ Week 6: RAG Systems — chunking, hybrid search, reranking
  ⬜ Week 7: LangChain & Python AI Microservices
  ⬜ Week 8: LangGraph — Stateful Agent Workflows
  ⬜ Week 9: AI Content Automation Platform (Phase 2 capstone)

Phase 3 (Weeks 10–12): AI Infrastructure & Production Systems
  ⬜ Week 10: Model Serving — vLLM & Ollama
  ⬜ Week 11: AI Observability & MLOps (LangSmith, W&B, MLflow)
  ⬜ Week 12: Capstone — AI Social Media Automation Platform
```

**My background advantage:** 7 years of Java / Apache Kafka / Spring Boot / Docker / Kubernetes in production means I can skip the infrastructure fundamentals and go straight into AI-specific skills. The goal is to become the engineer who can integrate AI into enterprise stacks — not just build Python scripts.

---

## Author

**Mahesh Annapureddy** — Senior Software Engineer → AI Infrastructure Engineer

Building in public. One project per week. All code on GitHub.
