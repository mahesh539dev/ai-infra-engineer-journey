# 🎯 MULTI-VECTOR DATABASE COMPARISON - DETAILED ASSIGNMENT

This assignment teaches you to work with **three production vector databases** and understand their tradeoffs. You'll build, test, and benchmark each one.

---

## 📚 LEARNING OBJECTIVES

By completing this assignment, you'll understand:
- ✅ How embeddings work and represent text as vectors
- ✅ How different vector databases store and search embeddings
- ✅ Performance characteristics of local vs. cloud vs. hybrid setups
- ✅ When to use each database in real-world applications
- ✅ How to benchmark and compare database performance

---

## PART 1: SETUP & UNDERSTANDING

### Assignment 1.1: Environment Setup
**What you need to do:**
1. Create a Python virtual environment
2. Install required packages:
   ```bash
   pip install chromadb pinecone-client psycopg2-binary pgvector sentence-transformers tabulate python-dotenv
   ```
3. Download the sentence transformer model: `all-MiniLM-L6-v2` (it auto-downloads on first use)
4. Create a `.env` file and add your Pinecone API key (if you have one; it's optional for testing)

**Questions to answer before coding:**
- [ ] What is an embedding? How many dimensions does `all-MiniLM-L6-v2` create?
- [ ] What's the difference between storing embeddings locally vs. in the cloud?

---

## PART 2: CHROMADB IMPLEMENTATION

### Assignment 2.1: Build a ChromaDB Collection
**Status:** ☐ Not Started | ◐ In Progress | ✅ Completed

**What you need to do:**
1. **Initialize ChromaDB**: Creates a persistent ChromaDB client pointing to `./chroma_test`
2. **Create a collection**: Named `"test"` with cosine similarity (`hnsw:space`)
3. **Prepare documents**: Transform the sample documents into the format ChromaDB expects
   - Extract IDs, text content, and metadata
4. **Insert documents**: Use `collection.add()` with all three parameters
5. **Measure insertion time**: Time how long it takes to insert 30 documents

**Expected output:**
```
✅ Inserted 30 documents in X.XXs
```

**Things to understand:**
- [ ] Why does ChromaDB handle embeddings automatically but Pinecone doesn't?
- [ ] What does `hnsw:space` mean? Why use "cosine" similarity?
- [ ] What metadata should travel with documents?

**Code structure hint:**
```python
# 1. Initialize client
client = chromadb.PersistentClient(path="./chroma_test")

# 2. Get or create collection
collection = client.get_or_create_collection(
    name="test", 
    metadata={"hnsw:space": "cosine"}
)

# 3. Prepare data from documents_data
ids = [doc["id"] for doc in documents_data]
documents = [doc["text"] for doc in documents_data]
metadatas = [doc["metadata"] for doc in documents_data]

# 4. Insert documents
start = time.time()
collection.add(ids=ids, documents=documents, metadatas=metadatas)
insert_time = time.time() - start

# 5. Print result
print(f"✅ Inserted {len(documents_data)} documents in {insert_time:.2f}s")
```

---

### Assignment 2.2: Query ChromaDB
**Status:** ☐ Not Started | ◐ In Progress | ✅ Completed

**What you need to do:**
1. Query your ChromaDB collection with text queries:
   - "machine learning and AI"
   - "database and vectors"
   - "programming and deployment"
2. **For each query:**
   - Measure the query latency (time in milliseconds)
   - Return the top 3 most similar documents
   - Print the results in a human-readable format

**Expected output:**
```
Query 'machine learning and AI': 15.23ms
  1. "Embeddings map text to high-dimensional vectors"
  2. "Deep neural networks learn hierarchical representations"
  3. "Machine learning requires large datasets for training"
```

**Questions to answer:**
- [ ] How does ChromaDB know which documents are similar without you manually creating embeddings?
- [ ] Why are the latencies measured in milliseconds for small datasets?

**Code structure hint:**
```python
test_queries = [
    "machine learning and AI",
    "database and vectors",
    "programming and deployment"
]

query_times = []
for query in test_queries:
    start = time.time()
    results = collection.query(query_texts=[query], n_results=3)
    query_time = time.time() - start
    query_times.append(query_time)
    
    print(f"Query '{query}': {query_time*1000:.2f}ms")
    for i, doc in enumerate(results['documents'][0]):
        print(f"  {i+1}. {doc}")
```

---

## PART 3: PINECONE IMPLEMENTATION (Optional - requires free account)

### Assignment 3.1: Set Up Pinecone Index
**Status:** ☐ Not Started | ◐ In Progress | ✅ Completed | ⊘ Skipped

**Prerequisites:**
- Create a free Pinecone account at https://www.pinecone.io (free tier available)
- Create an index called `"comparison-test"` with dimension **384** (matches all-MiniLM-L6-v2)
- Set similarity metric to **cosine**
- Add your API key to `.env` as `PINECONE_API_KEY`

**What you need to do:**
1. **Initialize Pinecone client**: Using your API key from environment
2. **Get the index**: Reference the "comparison-test" index
3. **Prepare vectors**: 
   - Use SentenceTransformer to encode each document's text to a 384-dim vector
   - Include the document ID and metadata with each vector
4. **Upsert in batches**: Insert vectors in batches of 100 (Pinecone best practice)
5. **Measure insertion time**: Total time for all insertions

**Expected output:**
```
✅ Inserted 30 documents in X.XXs
```

**Things to understand:**
- [ ] Why must you manually create embeddings for Pinecone but not for ChromaDB?
- [ ] Why batch upserts in groups of 100 instead of one at a time?
- [ ] What's the difference between "insert" and "upsert"?

**Code structure hint:**
```python
try:
    pc = Pinecone(api_key=api_key)
    index = pc.Index("comparison-test")
    
    # Prepare vectors with metadata
    vectors_to_upsert = [
        (
            doc["id"],
            model.encode(doc["text"]).tolist(),
            doc["metadata"]
        )
        for doc in documents_data
    ]
    
    # Upsert in batches of 100
    start = time.time()
    for i in range(0, len(vectors_to_upsert), 100):
        batch = vectors_to_upsert[i:i+100]
        index.upsert(vectors=batch)
    insert_time = time.time() - start
    
    print(f"✅ Inserted {len(documents_data)} documents in {insert_time:.2f}s")
    
except Exception as e:
    print(f"⚠️  Pinecone test skipped: {e}")
```

---

### Assignment 3.2: Query Pinecone
**Status:** ☐ Not Started | ◐ In Progress | ✅ Completed | ⊘ Skipped

**What you need to do:**
1. For each test query:
   - Encode the query text to a 384-dim vector
   - Query Pinecone for top 3 most similar documents
   - Measure latency and print results
   - Include metadata (topic and source)

**Expected output:**
```
Query 'machine learning and AI': 45.12ms
  1. doc_002 - "Embeddings map text..." (similarity: 0.87, topic: ai)
  2. doc_001 - "Deep neural networks..." (similarity: 0.84, topic: ai)
  3. doc_000 - "Machine learning requires..." (similarity: 0.81, topic: ai)
```

**Questions to answer:**
- [ ] Why is Pinecone's latency higher than ChromaDB's for small datasets?
- [ ] How would latency scale if you had 1 million documents?

**Code structure hint:**
```python
for query in test_queries:
    query_vector = model.encode(query).tolist()
    
    start = time.time()
    results = index.query(
        vector=query_vector,
        top_k=3,
        include_metadata=True
    )
    query_time = time.time() - start
    
    print(f"Query '{query}': {query_time*1000:.2f}ms")
    for match in results['matches']:
        print(f"  {match['id']} - (similarity: {match['score']:.2f})")
```

---

## PART 4: PGVECTOR IMPLEMENTATION

### Assignment 4.1: Set Up PostgreSQL with pgvector
**Status:** ☐ Not Started | ◐ In Progress | ✅ Completed

**Prerequisites:**
- Have PostgreSQL running locally (Docker recommended)
- pgvector extension installed

**Helpful Docker command:**
```bash
docker run --name postgres-pgvector \
  -e POSTGRES_PASSWORD=password \
  -p 5432:5432 \
  -d pgvector/pgvector:pg16
```

**What you need to do:**
1. **Connect to PostgreSQL**: Using psycopg2 with your credentials
2. **Register vector type**: Call `register_vector(conn)` to enable pgvector support
3. **Create table schema**:
   - `id` (VARCHAR, primary key)
   - `text` (TEXT)
   - `topic` (VARCHAR)
   - `source` (VARCHAR)
   - `embedding` (vector(384)) — exactly 384 dimensions
4. **Create HNSW index**: On the embedding column for fast search
   - Use `vector_cosine_ops` for cosine similarity
5. **Drop and recreate** on each run to ensure clean state

**Expected output:**
```
✅ Created table and index
```

**Things to understand:**
- [ ] What is HNSW indexing? Why is it faster than scanning all rows?
- [ ] Why must you specify the vector dimension (384) when creating the column?
- [ ] What's the difference between `vector_cosine_ops` and other similarity operators?

**Code structure hint:**
```python
try:
    conn = psycopg2.connect(
        host="localhost",
        database="postgres",
        user="postgres",
        password="password",
        port=5432
    )
    conn.autocommit = True
    cursor = conn.cursor()
    register_vector(conn)
    
    # Create table
    cursor.execute("""
    DROP TABLE IF EXISTS test_docs CASCADE;
    CREATE TABLE test_docs (
        id VARCHAR(50) PRIMARY KEY,
        text TEXT,
        topic VARCHAR(50),
        source VARCHAR(50),
        embedding vector(384)
    );
    CREATE INDEX ON test_docs USING hnsw (embedding vector_cosine_ops);
    """)
    
    print("✅ Created table and index")
    
except psycopg2.OperationalError as e:
    print(f"⚠️  PostgreSQL not available: {e}")
```

---

### Assignment 4.2: Insert Documents into pgvector
**Status:** ☐ Not Started | ◐ In Progress | ✅ Completed

**What you need to do:**
1. **For each document in documents_data:**
   - Encode the text to a 384-dim embedding using SentenceTransformer
   - Insert into the table with all metadata
2. **Commit the transaction** after all inserts
3. **Measure total insertion time**

**Expected output:**
```
✅ Inserted 30 documents in X.XXs
```

**Things to understand:**
- [ ] Why insert all documents before querying (commit timing)?
- [ ] How does prepared statement (`%s` placeholders) prevent SQL injection?
- [ ] Can you insert 30 documents in one query instead of 30 separate queries? What would be faster?

**Code structure hint:**
```python
start = time.time()
for doc in documents_data:
    embedding = model.encode(doc["text"])
    cursor.execute("""
    INSERT INTO test_docs (id, text, topic, source, embedding)
    VALUES (%s, %s, %s, %s, %s)
    """, (
        doc["id"],
        doc["text"],
        doc["metadata"]["topic"],
        doc["metadata"]["source"],
        embedding
    ))
conn.commit()
insert_time = time.time() - start

print(f"✅ Inserted {len(documents_data)} documents in {insert_time:.2f}s")
```

---

### Assignment 4.3: Query pgvector
**Status:** ☐ Not Started | ◐ In Progress | ✅ Completed

**What you need to do:**
1. **For each test query:**
   - Encode the query text to a 384-dim vector
   - Use the `<=>` operator to find cosine distance
   - Order by distance (smallest = most similar)
   - Limit to top 3 results
   - Calculate similarity as `1 - distance`
2. **Measure latency** and print results with similarity scores

**SQL pattern you'll use:**
```sql
SELECT id, text, 1 - (embedding <=> %s::vector) AS similarity
FROM test_docs
ORDER BY embedding <=> %s::vector
LIMIT 3
```

**Expected output:**
```
Query 'machine learning and AI': 8.34ms
  1. doc_002 - "Embeddings map..." (similarity: 0.87)
  2. doc_001 - "Deep neural..." (similarity: 0.84)
  3. doc_000 - "Machine learning..." (similarity: 0.81)
```

**Questions to answer:**
- [ ] What does the `<=>` operator do? Why use it instead of `-(embedding - query_vector)`?
- [ ] Why cast to `::vector` explicitly?
- [ ] How does the HNSW index speed up this query?

**Code structure hint:**
```python
for query in test_queries:
    query_embedding = model.encode(query)
    
    start = time.time()
    cursor.execute("""
    SELECT id, text, 1 - (embedding <=> %s::vector) AS similarity
    FROM test_docs
    ORDER BY embedding <=> %s::vector
    LIMIT 3
    """, (query_embedding, query_embedding))
    
    results = cursor.fetchall()
    query_time = time.time() - start
    
    print(f"Query '{query}': {query_time*1000:.2f}ms")
    for row in results:
        doc_id, text, similarity = row
        print(f"  {doc_id} - {text[:50]}... (similarity: {similarity:.2f})")
```

---

## PART 5: PERFORMANCE COMPARISON & ANALYSIS

### Assignment 5.1: Collect Metrics
**Status:** ☐ Not Started | ◐ In Progress | ✅ Completed

**What you need to do:**
1. **Store results** for each database:
   - Insert time (seconds)
   - Query latencies (list of milliseconds for each of 3 queries)
   - Average query latency
2. **Handle failures gracefully**:
   - If Pinecone API key is missing, skip with warning
   - If PostgreSQL is unavailable, skip with warning
   - Continue testing other databases

**Data structure you'll populate:**
```python
results = {
    "chromadb": {
        "insert_time": 0.15,
        "query_times": [12.3, 14.2, 13.8],
        "latency_avg": 0
    },
    "pinecone": {
        "insert_time": 2.45,
        "query_times": [45.1, 42.3, 48.2],
        "latency_avg": 0
    },
    "pgvector": {
        "insert_time": 0.82,
        "query_times": [6.5, 7.2, 6.8],
        "latency_avg": 0
    },
}

# Calculate averages
for db in results:
    if results[db]["query_times"] and results[db]["query_times"][0] != "N/A":
        results[db]["latency_avg"] = sum(results[db]["query_times"]) / len(results[db]["query_times"])
```

---

### Assignment 5.2: Generate Comparison Report
**Status:** ☐ Not Started | ◐ In Progress | ✅ Completed

**What you need to do:**
1. **Create a comparison table** showing:
   - Database name
   - Insert time (seconds)
   - Average query latency (milliseconds)
   - Best use case
2. **Print it nicely** using `tabulate` library
3. **Save detailed report** to `vectordb_comparison_report.json` with:
   - Test date and time
   - Number of documents tested
   - Full results object
   - Your recommendation for which database to use

**Expected output:**
```
DATABASE      | INSERT TIME  | AVG QUERY LATENCY | BEST FOR
ChromaDB      | 0.15s        | 13.43ms           | Development
Pinecone      | 2.45s        | 45.2ms            | Production Scale
pgvector      | 0.82s        | 6.83ms            | Existing Postgres
```

**Code structure hint:**
```python
# Create table data
comparison_data = [
    ["Database", "Insert Time", "Avg Query Latency", "Best For"],
    ["ChromaDB", f"{results['chromadb']['insert_time']:.2f}s", 
     f"{results['chromadb']['latency_avg']*1000:.2f}ms", "Development"],
    # ... add other databases
]

print(tabulate(comparison_data, tablefmt="grid"))

# Save report
report = {
    "test_date": time.strftime("%Y-%m-%d %H:%M:%S"),
    "documents_tested": len(documents_data),
    "queries_per_db": 3,
    "results": results
}

with open("vectordb_comparison_report.json", "w") as f:
    json.dump(report, f, indent=2)

print("📊 Full report saved to vectordb_comparison_report.json")
```

---

## PART 6: ANALYSIS & INTERPRETATION

### Assignment 6.1: Answer Reflection Questions
**Status:** ☐ Not Started | ◐ In Progress | ✅ Completed

**After running your code, answer these questions in a text file or comments:**

1. **Performance Analysis:**
   - [ ] Which database was fastest for queries? Why?
   - [ ] Which was fastest for inserts? Why?
   - [ ] How would results change with 10,000 documents? 1 million?

2. **Architecture Tradeoffs:**
   - [ ] Why is ChromaDB latency so low? (Hint: where is the data?)
   - [ ] Why does Pinecone have higher latency? (Hint: network overhead)
   - [ ] When would pgvector outperform Pinecone?

3. **Real-World Scenarios:**
   - [ ] **Scenario A**: Building an AI assistant that runs locally on a laptop
     - Which database would you choose? Why?
   - [ ] **Scenario B**: Building a production SaaS app with 10+ QPS
     - Which database would you choose? Why?
   - [ ] **Scenario C**: You already have PostgreSQL managing your business data
     - Which database would you choose? Why?

4. **Embedding Quality:**
   - [ ] All three databases use the same embeddings (`all-MiniLM-L6-v2`)
   - [ ] Why do they still produce different query latencies?
   - [ ] How would results change if you used a larger model like `all-mpnet-base-v2`?

---

## PART 7: VALIDATION CHECKPOINT

### Assignment 7.1: Verify Your Implementation
**Status:** ☐ Not Started | ◐ In Progress | ✅ Completed

**Before submitting, ensure:**
- [ ] ChromaDB test runs without errors
- [ ] ChromaDB queries return results similar to sample documents
- [ ] Pinecone test runs OR gracefully skips with warning
- [ ] pgvector test runs (requires PostgreSQL)
- [ ] Comparison report saves to JSON
- [ ] All latencies are measured and printed
- [ ] No hardcoded passwords (use `.env`)

**Final validation code:**
```python
checkpoint = {
    "chromadb_working": results["chromadb"]["insert_time"] > 0,
    "pinecone_tested": results["pinecone"]["insert_time"] != "N/A",
    "pgvector_working": results["pgvector"]["insert_time"] != "N/A",
    "comparison_complete": True,
}

print("\nValidation Checklist:")
for check, passed in checkpoint.items():
    status = "✅" if passed else "⚠️"
    print(f"  {status} {check}")

all_passed = all(checkpoint.values())
print(f"\n{'✅ ASSIGNMENT COMPLETE' if all_passed else '⚠️ Some tests incomplete'}")
```

---

## HOW TO APPROACH THIS ASSIGNMENT

### **Step 1: Start with ChromaDB** (Easiest)
- [ ] No API keys needed
- [ ] No database setup
- [ ] Automatic embeddings
- [ ] Get something working first

### **Step 2: Then pgvector** (Medium - requires PostgreSQL)
- [ ] More control over embeddings and queries
- [ ] Learn SQL + vectors
- [ ] Understand HNSW indexing
- [ ] Great for your background

### **Step 3: Finally Pinecone** (Optional but valuable)
- [ ] Learn cloud infrastructure
- [ ] Understand API-based databases
- [ ] See production-scale considerations

### **Step 4: Compare & Analyze**
- [ ] Don't skip the reflection questions
- [ ] Understanding *why* matters more than running code

---

## EXPECTED OUTCOMES

When complete, you should be able to:
1. ✅ Explain how embeddings represent semantic meaning
2. ✅ Build vector search with 3 different databases
3. ✅ Measure and interpret performance metrics
4. ✅ Choose the right database for different scenarios
5. ✅ Debug connection issues and handle errors gracefully

---

## USEFUL RESOURCES

- **ChromaDB docs:** https://docs.trychroma.com
- **Pinecone docs:** https://docs.pinecone.io
- **pgvector docs:** https://github.com/pgvector/pgvector
- **Sentence Transformers:** https://www.sbert.net

---

## SUBMISSION CHECKLIST

**Files to have when complete:**
- [ ] `day5_multi_vectordb_comparison.py` (main implementation)
- [ ] `sample_documents.json` (document data)
- [ ] `vectordb_comparison_report.json` (generated report)
- [ ] `reflection_answers.txt` or comments in code (answers to questions)
- [ ] `.env` file (with API keys, not committed to git)

**Final verification:**
```bash
# Run your code
python day5_multi_vectordb_comparison.py

# Check output
cat vectordb_comparison_report.json
```

Good luck! Start with Part 1 setup, then work through ChromaDB first. 🚀
