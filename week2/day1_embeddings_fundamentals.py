from sentence_transformers import SentenceTransformer
import numpy as np
from sklearn.metrics.pairwise import cosine_similarity
import pandas as pd
from sklearn.decomposition import PCA
import matplotlib.pyplot as plt

model = SentenceTransformer('all-MiniLM-L6-v2')
embedding_dim = model.get_embedding_dimension()
print(f"Embedding dimension: {embedding_dim}")

documents = [
    "The cat sits on the mat",
    "A feline rests on the rug",
    "Python is a programming language",
    "Java is used for backend systems",
    "Machine learning requires training data",
    "Neural networks learn patterns",
]

print("\nEmbedding documents...")
embeddings = model.encode(documents)
print(f"Embeddings shape: {embeddings.shape}") 

norms = np.linalg.norm(embeddings, axis=1)
print(f"Embedding norms (should be ~1.0): {norms}")

print("\n" + "="*60)
print("SIMILARITY MATRIX (Cosine)")
print("="*60)

sim_matrix = cosine_similarity(embeddings)

df = pd.DataFrame(sim_matrix,
                    index=[f"Doc {i}: {doc[:30]}..." for i, doc in enumerate(documents)],
                    columns=[f"Doc {i}" for i in range(len(documents))])
print(df.round(3))

print("\n" + "="*60)
print("SEMANTIC SIMILARITY EXAMPLES")
print("="*60)

def find_similar(query_text, embeddings, documents,model, top_k=2):
    query_embeddings = model.encode(query_text)
    similarities = cosine_similarity([query_embeddings],embeddings)[0]
    
    top_indices = np.argsort(similarities)[-top_k:][::-1]

    print(f"\nQuery: '{query_text}'")
    for rank, idx in enumerate(top_indices, 1):
        print(f"  {rank}. [{similarities[idx]:.3f}] {documents[idx]}")
    
    return similarities

queries = [
    "What about animals?",
    "Tell me about programming",
    "Deep learning and data"
]

for query in queries:
    find_similar(query, embeddings, documents, model, top_k=2)
    
print("\n" + "="*60)
print("DIMENSIONALITY REDUCTION FOR VISUALIZATION")
print("="*60)

pca = PCA(n_components=2)
embeddings_2d = pca.fit_transform(embeddings)
print(f"\nVariance explained by 2 dimensions: {pca.explained_variance_ratio_.sum():.1%}")

fig, ax = plt.subplots(figsize=(10, 8))
scatter = ax.scatter(embeddings_2d[:, 0], embeddings_2d[:, 1], s=200, alpha=0.6, c=range(len(documents)), cmap='viridis')

for i, doc in enumerate(documents):
    ax.annotate(f"Doc {i}", (embeddings_2d[i, 0], embeddings_2d[i, 1]), fontsize=8)

ax.set_title("Embeddings Visualization (PCA 2D)")
ax.set_xlabel("PC1")
ax.set_ylabel("PC2")
plt.tight_layout()
plt.savefig('embeddings_visualization.png')
print("✅ Saved embeddings_visualization.png")
