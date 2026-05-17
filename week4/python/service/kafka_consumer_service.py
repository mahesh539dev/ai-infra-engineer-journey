from kafka import KafkaConsumer
from dotenv import load_dotenv
import os
import json
import logging
import threading
from dataclasses import dataclass
from model.learning_ai_event import KafkaEvent
from model.rag_event import RagEvent
from core.measure_latency import track_latency
from service.embedding_service import EmbeddingService

load_dotenv()
BROKERS = os.getenv("BROKERS")
BATCH_SIZE = 100
logger = logging.getLogger(__name__)

embedding_service = EmbeddingService()


@dataclass
class TopicConfig:
    topic: str
    group_id: str


def _parse_topic_configs() -> list[TopicConfig]:
    raw = os.getenv("TOPIC_CONFIGS", "")
    configs = []
    for entry in raw.split(","):
        entry = entry.strip()
        if ":" not in entry:
            continue
        topic, group_id = entry.split(":", 1)
        configs.append(TopicConfig(topic=topic.strip(), group_id=group_id.strip()))
    return configs


# --- handlers: one function per topic ---

def _handle_learning_ai(messages: list, partitions: list, offsets: list):
    events = [KafkaEvent.model_validate(json.loads(m) if isinstance(m, str) else m) for m in messages]
    with track_latency("upsert_data", request_id=None, record_count=len(events)):
        embedding_service.upsert_data(events, partitions, offsets)


def _handle_rag_events(messages: list, partitions: list, offsets: list):  # noqa: ARG001
    events = [RagEvent.model_validate(json.loads(m) if isinstance(m, str) else m) for m in messages]
    with track_latency("upsert_data", request_id=None,record_count=len(events)):
        embedding_service.upsert_rag_data(events, partitions, offsets)


_HANDLERS = {
    "learning-ai": _handle_learning_ai,
    "rag-events": _handle_rag_events,
}


# --- generic consumer wiring ---

def _create_consumer(config: TopicConfig) -> KafkaConsumer:
    return KafkaConsumer(
        config.topic,
        bootstrap_servers=[BROKERS],
        value_deserializer=lambda x: x.decode("utf-8"),
        group_id=config.group_id,
        auto_offset_reset="earliest",
        max_poll_records=BATCH_SIZE,
    )


def _run_consumer(config: TopicConfig):
    handler = _HANDLERS.get(config.topic)
    if handler is None:
        logger.error(f"No handler registered for topic '{config.topic}' — skipping")
        return

    consumer = _create_consumer(config)
    logger.info(f"Consumer started: topic={config.topic} group={config.group_id}")
    try:
        while True:
            batch = consumer.poll(timeout_ms=1000)
            if not batch:
                continue
            flat = [(msg.value, msg.partition, msg.offset) for msgs in batch.values() for msg in msgs]
            messages  = [v for v, _, _ in flat]
            partitions = [p for _, p, _ in flat]
            offsets    = [o for _, _, o in flat]
            logger.info(f"[{config.topic}] processing batch of {len(messages)}")
            handler(messages, partitions, offsets)
    except KeyboardInterrupt:
        logger.info(f"[{config.topic}] consumer stopped by user")
    finally:
        consumer.close()
        logger.info(f"[{config.topic}] consumer closed")


def run_all_consumers():
    configs = _parse_topic_configs()
    if not configs:
        logger.error("No topic configs found — check TOPIC_CONFIGS in .env")
        return

    threads = []
    for config in configs:
        t = threading.Thread(target=_run_consumer, args=(config,), name=f"consumer-{config.topic}", daemon=True)
        t.start()
        threads.append(t)
        logger.info(f"Launched thread for topic '{config.topic}'")

    for t in threads:
        while t.is_alive():
            t.join(timeout=1.0)
