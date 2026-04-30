import chromadb
from sentence_transformers import SentenceTransformer
import json
from pathlib import Path

client = chromadb.PersistentClient(path="./chroma_data")

collection = client.get_or_create_collection(
    name="week2_learning",
    metadata={"hnsw:space": "cosine"}
    )

print(f"Collection created: {collection.name}")

data_file = Path("day2_learn_data.json")
with open(data_file, "r") as f:
    documents_data = json.load(f)

print("\nAdding documents to ChromaDB...")

ids = [doc["id"] for doc in documents_data]
documents = [doc["text"] for doc in documents_data]
metadatas = [doc["metadata"] for doc in documents_data]

collection.add(
    ids=ids,
    documents=documents,
    metadatas=metadatas,
)

print(f"✅ Added {len(documents)} documents to ChromaDB")

count = collection.count()
print(f"Total documents in collection: {count}")

print("\n" + "="*60)
print("SEMANTIC SEARCH QUERIES")
print("="*60)

def search(query_text,n_results=3):
    results = collection.query(
        query_texts=[query_text],
        n_results=n_results
    )
        
    print(f"\nQuery: '{query_text}'")--
    print(f"Results (top {n_results}):")
    print(f"Results {results}")    
    for i in range(len(results['ids'][0])):
        doc_id = results['ids'][0][i]
        doc_text = results['documents'][0][i]
        distance = results['distances'][0][i]
        metadata = results['metadatas'][0][i]
        
        # Distance in ChromaDB is sometimes L2 or cosine depending on config
        # For cosine with normalized vectors: similarity = 1 - distance
        similarity = 1 - distance if distance < 1 else distance
        
        print(f"\n  {i+1}. [{similarity:.3f}] {doc_text[:60]}...")
        print(f"     ID: {doc_id}, Topic: {metadata['topic']}")
    
    return results

test_queries = [
    "How do machine learning algorithms work?",
    "What is a vector database?",
    "Tell me about Python programming",
    "How are transformers used in AI?",
]

for query in test_queries:
    search(query, n_results=3)

print("\n" + "="*60)
print("FILTERED SEARCH (Only AI topic)")
print("="*60)

results = collection.query(
    query_texts=["machine learning"],
    n_results=5,
    where={"topic": "ai"}  # Filter by metadata
)

print("\nResults filtered to topic='ai':")
for i, (doc_id, doc_text) in enumerate(zip(results['ids'][0], results['documents'][0]), 1):
    print(f"  {i}. {doc_id} {doc_text[:60]}...")

print("\n" + "="*60)
print("CRUD OPERATIONS")
print("="*60)

# Update a document
collection.update(
    ids=["doc_001"],
    documents=["Python 3.11 is the latest stable version of Python"],
    metadatas=[{"source": "tutorial", "topic": "programming"}]
)
print("✅ Updated doc_001")

# Delete a document
collection.delete(ids=["doc_006"])
print("✅ Deleted doc_006 (FastAPI)")
print(f"Total documents now: {collection.count()}")

with open('chromadb_info.json', 'w') as f:
    json.dump({
        'collection_name': collection.name,
        'total_docs': collection.count(),
        'model': 'default (sentence-transformers)',
        'distance_metric': 'cosine'
    }, f, indent=2)

print("\n✅ Session complete. Data persisted to ./chroma_data/")