from pinecone import Pinecone
import pandas as pd
import numpy as np
from dotenv import load_dotenv
import os
import time
from sentence_transformers import SentenceTransformer

load_dotenv()
api_key = os.getenv("PINECONE_API_KEY")

print(api_key)
pc = Pinecone(api_key=api_key)
index_name = "day3-week2-semantic-search"
existing_indexes = [idx.name for idx in pc.list_indexes()]
print(f"Existing indexes: {existing_indexes}")
model = SentenceTransformer('all-MiniLM-L6-v2')


if not pc.has_index(index_name):
    pc.create_index(
        name=index_name,
        dimension=384,  # all-MiniLM-L6-v2 dimension
        metric="cosine",
        spec={
            "serverless": {
                "cloud": "aws",
                "region": "us-east-1"
            }
        }
    )

    # Wait for index to be ready
while not pc.describe_index(index_name).status.ready:
    time.sleep(1)
    print("✅ Index is ready")
else:
    print(f"✅ Index '{index_name}' already exists")

index = pc.Index(index_name)

documents_data = [
    {
        "id":f"doc_{i:03d}",
        "text":text,
        "metadata":{
            "source":source,
            "topic":topic,
            "length": len(text)
        }
    }
    for i,(text,source,topic) in enumerate([
        ("Python is a versatile programming language used in data science", "tutorial", "programming"),
        ("Machine learning models learn patterns from data", "research", "ai"),
        ("Vector databases enable similarity search at scale", "blog", "databases"),
        ("Transformers revolutionized natural language processing", "research", "nlp"),
        ("Deep learning uses neural networks with multiple layers", "course", "ai"),
        ("Pinecone provides serverless vector database solutions", "documentation", "databases"),
        ("Semantic search finds contextually relevant results", "blog", "nlp"),
        ("Retrieval Augmented Generation improves LLM accuracy", "research", "ai"),
        ("FastAPI simplifies building Python REST APIs", "tutorial", "programming"),
        ("Embeddings capture semantic meaning of text", "blog", "ai"),
    ])
]

print(f"Prepared {len(documents_data)} documents")

# 4. Embed and upsert to Pinecone (20 min)
print("\nEmbedding and upserting documents...")

vectors_to_upsert = []
for doc in documents_data:
    embedding = model.encode(doc['text']).tolist()
    vectors_to_upsert.append((
        doc['id'],
        embedding,
        doc['metadata']
    ))

# Upsert in batches (best practice for production)
batch_size = 100
for i in range(0, len(vectors_to_upsert), batch_size):
    batch = vectors_to_upsert[i:i+batch_size]
    index.upsert(vectors=batch)
    print(f"  Upserted batch {i//batch_size + 1}")

print("\n" + "="*60)
print("SEMANTIC SEARCH ON PINECONE")
print("="*60)

def search_pinecone(query_text, top_k=3):
    """Query Pinecone for similar documents"""
    query_embedding = model.encode(query_text).tolist()
    
    results = index.query(
        vector=query_embedding,
        top_k=top_k,
        include_metadata=True
    )
    
    print(f"\nQuery: '{query_text}'")
    print(f"Results (top {top_k}):")
    
    for i, match in enumerate(results['matches'], 1):
        print(f"\n  {i}. [{match['score']:.3f}] ID: {match['id']}")
        print(f"     Metadata: {match['metadata']}")
    
    return results

test_queries = [
    "How does machine learning work?",
    "Tell me about vector databases",
    "What is semantic search?",
]

for query in test_queries:
    search_pinecone(query, top_k=2)

print("\n" + "="*60)
print("FILTERED SEARCH (AI topic only)")
print("="*60)

query_text = "data and learning"
query_embedding = model.encode(query_text).tolist()

results = index.query(
    vector=query_embedding,
    top_k=5,
    filter={"topic": {"$eq": "ai"}},
    include_metadata=True
)

print(f"\nFiltered results (topic='ai'):")
for i, match in enumerate(results['matches'], 1):
    print(f"  {i}. {match['id']} - {match['metadata'].get('text', '')[:50]}...")

print("\n" + "="*60)
print("INDEX STATISTICS")
print("="*60)

stats = index.describe_index_stats()
print(f"Total vectors: {stats.total_vector_count}")
print(f"Dimension: {stats.dimension}")
print(f"Index fullness: {stats.indexFullness}")

# Save session info
with open('pinecone_session.txt', 'w') as f:
    f.write(f"Index: {index_name}\n")
    f.write(f"Total vectors: {stats.total_vector_count}\n")
    #f.write(f"API endpoint ready at: {index.host}\n")

print("\n✅ Pinecone session complete")

print("\n" + "="*60)
print("DELETE USING METADATA")
print("="*60)

index.delete(
    filter={
        "length" : {"$gt" : 60}
    }
)

print(f"Sentences with more than 100 characters are deleted")