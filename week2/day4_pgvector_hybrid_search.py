import os
from dotenv import load_dotenv
from sentence_transformers import SentenceTransformer
import psycopg2
from psycopg2.extras import execute_values
from pgvector.psycopg2 import register_vector

load_dotenv()

# 1. Connect to Postgres (10 min)
print("Connecting to Postgres...")

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
    print("✅ Connected to Postgres")

    # Create pgvector extension
    cursor.execute("CREATE EXTENSION IF NOT EXISTS vector")
    print("✅ pgvector extension created")

    # Register pgvector
    register_vector(conn)
    
except psycopg2.OperationalError as e:
    print(f"❌ Connection failed: {e}")
    print("Make sure Postgres is running with pgvector extension")
    exit(1)
    
print("\nCreating documents table with vector support...")

create_table_query = """
DROP TABLE IF EXISTS documents CASCADE;

CREATE TABLE documents (
    id SERIAL PRIMARY KEY,
    doc_id VARCHAR(50) UNIQUE NOT NULL,
    title VARCHAR(255) NOT NULL,
    content TEXT NOT NULL,
    topic VARCHAR(50),
    source VARCHAR(50),
    embedding vector(384),  -- Matches all-MiniLM-L6-v2 dimension
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create HNSW index for faster similarity search
CREATE INDEX ON documents USING hnsw (embedding vector_cosine_ops) 
WITH (m=5, ef_construction=200);

-- Create BTree index for keyword search
CREATE INDEX ON documents (topic);
"""

cursor.execute(create_table_query)
print("✅ Table created with vector index")

print("\nLoading embedding model...")
model = SentenceTransformer('all-MiniLM-L6-v2')

print("\nInserting documents...")

documents_data = [
    ("doc_001", "Python Basics", "Python is a high-level programming language", "programming", "tutorial"),
    ("doc_002", "ML Fundamentals", "Machine learning models are trained on data", "ai", "research"),
    ("doc_003", "Vector DBs", "Vector databases enable similarity search at scale", "databases", "blog"),
    ("doc_004", "Transformers", "Transformers revolutionized NLP with attention mechanisms", "nlp", "research"),
    ("doc_005", "Deep Learning", "Deep learning uses neural networks with multiple layers", "ai", "course"),
    ("doc_006", "Pinecone", "Pinecone is a managed vector database service", "databases", "docs"),
    ("doc_007", "Semantic Search", "Semantic search finds contextually relevant documents", "nlp", "blog"),
    ("doc_008", "RAG Systems", "Retrieval Augmented Generation improves LLM accuracy", "ai", "research"),
    ("doc_009", "FastAPI", "FastAPI simplifies building Python REST APIs", "programming", "tutorial"),
    ("doc_010", "Embeddings", "Embeddings capture semantic meaning in vector form", "ai", "blog"),
]

insert_query = """
INSERT INTO documents (doc_id, title, content, topic, source, embedding)
VALUES %s
ON CONFLICT (doc_id) DO UPDATE SET embedding = EXCLUDED.embedding;
"""

# Prepare data with embeddings
values = []
for doc_id, title, content, topic, source in documents_data:
    embedding = model.encode(content)
    values.append((doc_id, title, content, topic, source, embedding))

execute_values(cursor, insert_query, values)
conn.commit()
print(f"✅ Inserted {len(values)} documents with embeddings")

print("\n" + "="*60)
print("VECTOR SIMILARITY SEARCH")
print("="*60)

def vector_search(query_text, limit=3):
    """Find similar documents using vector similarity"""
    query_embedding = model.encode(query_text)
    
    # PostgreSQL <-> operator = cosine distance
    # Smaller distance = more similar
    search_query = """
    SELECT 
        doc_id, 
        title, 
        content, 
        topic,
        1 - (embedding <=> %s::vector) AS similarity
    FROM documents
    ORDER BY embedding <=> %s::vector
    LIMIT %s;
    """
    
    cursor.execute(search_query, (query_embedding, query_embedding, limit))
    results = cursor.fetchall()
    
    print(f"\nQuery: '{query_text}'")
    print(f"Results (top {limit}):")
    
    for i, (doc_id, title, content, topic, similarity) in enumerate(results, 1):
        print(f"  {i}. [{similarity:.3f}] {title} ({topic})")
        print(f"     {content[:60]}...")
    
    return results

# Test searches
test_queries = [
    "How do machine learning models work?",
    "Tell me about vector databases",
    "What is semantic search?",
]

for query in test_queries:
    vector_search(query, limit=2)


print("\n" + "="*60)
print("HYBRID SEARCH (Keyword + Semantic)")
print("="*60)

def hybrid_search(query_text, topic_filter=None, limit=3):
    """Combined keyword filtering + semantic search"""
    query_embedding = model.encode(query_text)
    
    where_clause = ""
    params = [query_embedding, query_embedding]
    
    if topic_filter:
        where_clause = "WHERE topic = %s"
        params = [query_embedding, topic_filter, query_embedding]
    
    # WITHOUT clause applies filters before similarity calculation
    hybrid_query = f"""
    SELECT 
        doc_id, 
        title, 
        content, 
        topic,
        1 - (embedding <=> %s::vector) AS similarity
    FROM documents
    {where_clause}
    ORDER BY embedding <=> %s::vector
    LIMIT %s;
    """
    
    params.append(limit)
    cursor.execute(hybrid_query, params)
    results = cursor.fetchall()
    
    print(f"\nHybrid Search: '{query_text}' {f'(topic={topic_filter})' if topic_filter else ''}")
    print(f"Results:")
    
    for i, (doc_id, title, content, topic, similarity) in enumerate(results, 1):
        print(f"  {i}. [{similarity:.3f}] {title}")
    
    return results

hybrid_search("learning and data", topic_filter="ai", limit=3)

print("\n" + "="*60)
print("TABLE STATISTICS")
print("="*60)

cursor.execute("SELECT COUNT(*) FROM documents;")
count = cursor.fetchone()[0]
print(f"Total documents: {count}")

cursor.execute("""
SELECT topic, COUNT(*) 
FROM documents 
GROUP BY topic 
ORDER BY COUNT(*) DESC;
""")

print("\nDocuments by topic:")
for topic, doc_count in cursor.fetchall():
    print(f"  {topic}: {doc_count}")

# 8. Update documents (5 min)
print("\n" + "="*60)
print("UPDATE DOCUMENT")
print("="*60)

update_query = """
UPDATE documents 
SET content = %s, embedding = %s 
WHERE doc_id = %s;
"""

new_content = "Python 3.12 is the latest version with improved performance"
new_embedding = model.encode(new_content)

cursor.execute(update_query, (new_content, new_embedding, "doc_001"))
conn.commit()
print("✅ Updated doc_001")

# Close connections
cursor.close()
conn.close()
print("\n✅ Postgres session complete")