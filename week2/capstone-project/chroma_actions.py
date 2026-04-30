import chromadb
import fileDataReader

client = chromadb.PersistentClient(path="./chroma_capstone")
collection = client.get_or_create_collection(
    name="capstone-week2",
    configuration={
        "hnsw": {
            "space": "cosine",
            "ef_construction": 200,
            "ef_search" : 200
        }
    }
)

def load_data_to_chroma():
    pdf_chunks = fileDataReader.extract_pdf_text_in_chunks_with_metadat("capstone_test_document.pdf")

    ids = pdf_chunks["ids"]
    documents = pdf_chunks["text"]
    metadatas = pdf_chunks["metadatas"]

    collection.add(
        ids=ids,
        documents=documents,
        metadatas=metadatas
    )

def query_data(query_text,num_of_results):
    results = collection.query(
        query_texts=query_text,
        n_results=num_of_results
    )
    polished_results = {}
    for i,(id,document,metadata,distance) in enumerate(zip(results["ids"][0],results["documents"][0],results["metadatas"][0],results["distances"][0]),1):
        similarity = 1 - distance
        polished_results[f"result_{i}"] = {
            "text": document,
            "metadata": metadata,
            "similarity": round(similarity, 4)
        }
    
    return polished_results