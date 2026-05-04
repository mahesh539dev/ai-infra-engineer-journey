from pinecone import Pinecone
from dotenv import load_dotenv
from sentence_transformers import SentenceTransformer
import os
import time
import json
from learning_ai_event import KafkaEvent

load_dotenv()
pinecone_key=os.getenv("PINECONE_API_KEY")

pc = Pinecone(api_key=pinecone_key)
index_name = "week3"
batch_size = 100
if not pc.has_index(index_name):
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
    
index = pc.Index(index_name)
model = SentenceTransformer('all-MiniLM-L6-v2')

def upsert_data(eventList: list[KafkaEvent], offsets: list[int]):
    vectors_to_upsert = [
        {
            "id": f"{event.topic}-{offset}",
            "values": model.encode(event.message).tolist(),
            "metadata": {
                "topic": event.topic,
                "subtopic": event.subtopic
            }
        }
        for event, offset in zip(eventList, offsets)
    ]
    
    for i in range(0, len(vectors_to_upsert), batch_size):
        batch = vectors_to_upsert[i:i+batch_size]
        index.upsert(vectors=batch)
    print("Inserted Data Successfully")
    return "Inserted Data Successfully"