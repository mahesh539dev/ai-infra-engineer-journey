from core.logging_config import setup_logging
from service.kafka_consumer_service import run_all_consumers

if __name__ == "__main__":
    setup_logging()
    run_all_consumers()

