import yaml
import logging
import threading
import signal
from utils.alpaca_utils import stream_starter
from utils.kafka_utils import create_kafka_admin, create_kafka_producer, start_kafka, stop_kafka


def main():

    logger = logging.getLogger(__name__)
    logging.basicConfig(
        format='%(asctime)s - %(levelname)s - %(message)s',
        encoding='utf-8', 
        level=logging.DEBUG
    )
    logger.info("Program started...")

    logger.info("Loading configs...")
    with open('configs/kafka_datasets.yaml', 'r') as f:
        CONFIG = yaml.safe_load(f)

    logger.info("Starting Kafka stream...")
    kafka_process = start_kafka()
    
    logger.info("Creating Kafka admin interface...")
    kafka_admin = create_kafka_admin()
    
    logger.info("Creating Kafka Producer (Alpaca)...")
    producer = create_kafka_producer()

    threads = []
    created_topics = []

    def shutdown(sig, frame):
        logger.info("Shutting down...")
        producer.flush()
        kafka_admin.delete_topics(created_topics)
        stop_kafka(kafka_process)
        exit(0)

    signal.signal(signal.SIGINT, shutdown)   # Ctrl+C
    signal.signal(signal.SIGTERM, shutdown)  # kill command

    for data_stream, subscription_config in CONFIG.items():

        t = threading.Thread(
            target=stream_starter, 
            args=(
                data_stream, 
                subscription_config, 
                producer, 
                kafka_admin,
                created_topics
            ),
            kwargs={"test_mode": True}
        )
        threads.append(t)
                
        # Start each thread
    for t in threads:
        t.daemon= True
        t.start()

    signal.pause()


    return


main()