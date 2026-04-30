from sentence_transformers import SentenceTransformer
import os
from dotenv import load_dotenv
from pinecone import Pinecone
import chromadb
import psycopg2
from psycopg2.extras import execute_values
from pgvector.psycopg2 import register_vector
import time
import json
from collections import defaultdict
from tabulate import tabulate

#Loading required properties and model
load_dotenv()
api_key = os.getenv("PINECONE_API_KEY")
model = SentenceTransformer('all-MiniLM-L6-v2')

num_results = 3
results = defaultdict(lambda: {"insert_time": 0, "query_times": [], "latency_avg": 0})
print("\n" + "="*60)
print("LOAD DOCUMETS DATA")
print("="*60)

documents_data = []
with open("sample_documents.json") as f:
    data = json.loads(f.read())
    documents_data = data['documents']

test_queries = [
    "machine learning and AI",
    "database and vectors",
    "programming and deployment"
]

def get_document_by_id(doc_id):
    for doc in documents_data:
        if doc["id"] == doc_id:
            return doc
    return None
print("✅ COMPLETED READING DOCUMETS DATA")

print("\n" + "="*60)
print("CHROMA DB TEST")
print("="*60)
print("INSERT CHROMA DB TEST")
print("-"*60)
client = chromadb.PersistentClient(path="/chroma_test")
collection = client.get_or_create_collection(
    name="test",
     configuration={
        "hnsw": {
            "space": "cosine"
        }
    }
)

ids = [doc["id"] for doc in documents_data]
documents = [doc["text"] for doc in documents_data]
metadatas = [doc["metadata"] for doc in documents_data]
start = time.time()
collection.add(
    ids = ids,
    documents=documents,
    metadatas=metadatas
)
insert_time = time.time() - start

results["chromadb"]["insert_time"] = round(insert_time,2)

print(f"✅ Inserted {len(documents_data)} documents in {insert_time:.2f}s")

print("QUERY CHROMA DB TEST")
print("-"*60)

def search_chromadb(query_text):
    chroma_query_start = time.time()
    chroma_query_results = collection.query(query_texts=[query_text], n_results=num_results)
    chroma_query_time = time.time() - chroma_query_start
    results["chromadb"]["query_times"].append(round(chroma_query_time*1000,2))
    print(f"Query '{query_text}': {chroma_query_time*1000:.2f}ms")
    for i, (doc_id, document) in enumerate(zip(chroma_query_results["ids"][0], chroma_query_results["documents"][0]),1):
        print(f"  {i}. {doc_id} - {document}")

for query_text in test_queries:
    search_chromadb(query_text)
    
print("\n" + "="*60)
print("END OF CHROMA DB TEST")
print("="*60)

print("\n" + "="*60)
print("PINECONE DB TEST")
print("="*60)
print("INSERT PINECONE DB TEST")
print("-"*60)

pc = Pinecone(api_key=api_key)
index_name = "comparison-test"
existing_indexes = [idx.name for idx in pc.list_indexes()]

if index_name not in existing_indexes:
    pc.create_index(
        name=index_name,
        dimension=384,
        metric="cosine",
        spec={
            "serverless": {
                "cloud": "aws",
                "region": "us-east-1"
            }
        }
    )
while not pc.describe_index(index_name).status.ready:
    time.sleep(1)
    print("✅ Index is ready")
else:
    print(f"✅ Index '{index_name}' already exists")
index = pc.Index(index_name)
vectors_to_upsert = [
        (
            doc["id"],
            model.encode(doc["text"]).tolist(),
            doc["metadata"]
        )
        for doc in documents_data
    ]
batch_size = 100
pinecone_insert_start_time = time.time()

for i in range(0, len(vectors_to_upsert), batch_size):
    batch = vectors_to_upsert[i:i+batch_size]
    index.upsert(vectors=batch)
pinecone_insert_time = time.time() - pinecone_insert_start_time
results["pinecone"]["insert_time"] = round(pinecone_insert_time,2)
print(f"✅ Inserted {len(documents_data)} documents in {pinecone_insert_time:.2f}s")

print("QUERY PINECONE DB TEST")
print("-"*60)

def search_pinecone_test(query_embeddings):
    pinecone_query_start_time = time.time()
    results_pinecone = index.query(
        vector = query_embeddings,
        top_k=num_results
    )
    pinecone_query_time = time.time() - pinecone_query_start_time
    results["pinecone"]["query_times"].append(round(pinecone_query_time*1000,2))
    print(f"Query '{query}': {pinecone_query_time*1000:.2f}ms")
    for i,match in enumerate(results_pinecone['matches'],1):
        doc = get_document_by_id(match['id'])
        print(f"  {match['id']} - {doc['text'][:60]}... - (similarity: {match['score']:.2f})")

for query in test_queries:
    search_pinecone_test(model.encode(query).tolist())
    
print("\n" + "="*60)
print("END OF PINECONE DB TEST")
print("="*60)

print("\n" + "="*60)
print("PG Vector DB TEST")
print("="*60)
print("INSERT PG Vector DB TEST")
print("-"*60)

try:
    conn = psycopg2.connect(
        host="localhost",
        database="postgres",
        user="postgres",
        password="postgres",
        port=5434
    )
    conn.autocommit = True
    cursor = conn.cursor()
    # Create pgvector extension
    cursor.execute("CREATE EXTENSION IF NOT EXISTS vector")

    # Register pgvector
    register_vector(conn)
    
except psycopg2.OperationalError as e:
    print(f"❌ Connection failed: {e}")
    print("Make sure Postgres is running with pgvector extension")
    exit(1)

create_table_query = """
DROP TABLE IF EXISTS comparisontest CASCADE;

CREATE TABLE comparisontest (
    id SERIAL PRIMARY KEY,
    doc_id VARCHAR(50) UNIQUE NOT NULL,
    text TEXT NOT NULL,
    topic VARCHAR(50),
    source VARCHAR(50),
    embedding vector(384),  -- Matches all-MiniLM-L6-v2 dimension
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create HNSW index for faster similarity search
CREATE INDEX ON comparisontest USING hnsw (embedding vector_cosine_ops) 
WITH (m=5, ef_construction=200);

"""

cursor.execute(create_table_query)

insert_query = """
INSERT INTO comparisontest (doc_id, text, topic, source, embedding)
VALUES %s
ON CONFLICT (doc_id) DO UPDATE SET embedding = EXCLUDED.embedding;
"""
values = []

pgvector_insert_start_time = time.time()

for doc in documents_data:
    embedding = model.encode(doc['text'])
    values.append((doc['id'], doc['text'], doc['metadata']['topic'], doc['metadata']['source'], embedding))
    
    
execute_values(cursor, insert_query, values)
conn.commit()
pgvector_insert_time = time.time() - pgvector_insert_start_time
results["pgvector"]["insert_time"] = round(pgvector_insert_time,2)
print(f"✅ Inserted {len(values)} documents with embeddings in {pgvector_insert_time:.2f}s")

print("-"*60)
print("QUERY PG Vector DB TEST")
print("-"*60)

def pgvector_search(query_text):
    query_embedding = model.encode(query_text)

    search_query = """
    SELECT
        doc_id,
        text,
        topic,
        1 - (embedding <=> %s::vector) AS similarity
    FROM comparisontest
    ORDER BY embedding <=> %s::vector
    LIMIT %s;
    """
    pgvector_query_start_time = time.time()
    cursor.execute(search_query, (query_embedding, query_embedding, num_results))
    pgresults = cursor.fetchall()
    pgvector_query_time = time.time() - pgvector_query_start_time
    results["pgvector"]["query_times"].append(round(pgvector_query_time*1000,2))
    print(f"Query '{query_text}': {pgvector_query_time*1000:.2f}ms")
    for i, (doc_id, text, topic, similarity) in enumerate(pgresults, 1):
        print(f"  {i}. {doc_id} - {text[:60]}... (similarity: {similarity:.3f})")
    return pgresults
for query in test_queries:
    pgvector_search(query)

print("\n" + "="*60)
print("END OF PGVECTOR DB TEST")
print("="*60)

# Calculate averages
for db in results:
    if results[db]["query_times"] and results[db]["query_times"][0] != "N/A":
        results[db]["latency_avg"] = sum(results[db]["query_times"]) / len(results[db]["query_times"])
        
print(dict(results))

# Create table data
comparison_data = [
    ["Database", "Insert Time (s)", "Avg Query Latency (ms)"],
    ["ChromaDB", f"{results['chromadb']['insert_time']:.2f}", f"{results['chromadb']['latency_avg']:.2f}"],
    ["Pinecone", f"{results['pinecone']['insert_time']:.2f}", f"{results['pinecone']['latency_avg']:.2f}"],
    ["pgvector", f"{results['pgvector']['insert_time']:.2f}", f"{results['pgvector']['latency_avg']:.2f}"],
]

print(tabulate(comparison_data, headers="firstrow", tablefmt="grid"))

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