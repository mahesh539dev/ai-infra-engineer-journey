from contextlib import asynccontextmanager
from fastapi import FastAPI
from pipeline import generate_clean_data
import json
from sentence_transformers import SentenceTransformer
import pandas as pd
from sklearn.metrics.pairwise import cosine_similarity
import numpy as np

model = None
names = None
name_embeddings = None

@asynccontextmanager
async def lifespan(_: FastAPI):
    global model, names, name_embeddings
    generate_clean_data()
    model = SentenceTransformer("sentence-transformers/all-MiniLM-L6-v2")
    csv_data = pd.read_csv('Titanic-Dataset.csv')
    names = csv_data["Name"].tolist()
    name_embeddings = model.encode(names)
    yield

app = FastAPI(lifespan=lifespan)

@app.get("/stats")
def print_data():
    return read_stats()

@app.get("/survival-rate")
def get_survival_rate():
    return read_stats()["survival_rate_percentage"]

@app.get("/by-gender")
def survival_by_gender():
    return read_stats()["survival_count_by_gender"]

@app.get("/similar-name")
def get_similar_names(name: str):
    if model is None or name_embeddings is None:
        return {"error": "Model not initialized yet"}

    query_embedding = model.encode(name).reshape(1, -1)
    similarities = cosine_similarity(query_embedding, name_embeddings)[0]
    top_3_indices = np.argsort(similarities)[-3:][::-1]

    results = [
        {"name": names[i], "similarity": float(similarities[i])}
        for i in top_3_indices
    ]
    return results

def read_stats():
    f = open("stats.json")
    json_raw_data = f.read()
    json_data = json.loads(json_raw_data)
    return json_data