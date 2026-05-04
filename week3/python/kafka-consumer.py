# Simple Kafka Consumer
from kafka import KafkaConsumer
from dotenv import load_dotenv
import os
from learning_ai_event import KafkaEvent
from pinecone_insertion import upsert_data

load_dotenv()
topic_name = os.getenv("TOPIC_DATA")
brokers = os.getenv("BROKERS")
BATCH_SIZE = 100

def create_consumer():
    consumer = KafkaConsumer(
        topic_name,
        bootstrap_servers=[brokers],
        value_deserializer=lambda x: KafkaEvent.model_validate_json(x.decode('utf-8')),
        group_id='learning-ai-cg1',
        auto_offset_reset='earliest',
        max_poll_records=BATCH_SIZE,
    )
    return consumer

def run_consumer():
    consumer = create_consumer()

    try:
        print("Consumer started. Waiting for messages...")
        while True:
            batch = consumer.poll(timeout_ms=1000)
            if not batch:
                continue
            messages_flat = [(msg.value, msg.offset) for messages in batch.values() for msg in messages]
            events = [event for event, _ in messages_flat]
            offsets = [offset for _, offset in messages_flat]
            print(f"Processing batch of {len(events)} messages...")
            upsert_data(events, offsets)
    except KeyboardInterrupt:
        print("Consumer stopped by user")
    finally:
        consumer.close()
        print("Consumer closed")

# Main execution
if __name__ == "__main__":
    # To run as a producer, uncomment:
    # run_producer()
    
    # To run as a consumer, uncomment:
    run_consumer()