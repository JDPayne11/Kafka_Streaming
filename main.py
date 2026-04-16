import yaml
from utils.alpaca_utils import stream_starter
from utils.kafka_utils import create_kafka_admin, create_topics, create_kafka_producer, start_kafka, stop_kafka

def main():

    with open('configs/kafka_datasets.yaml', 'r') as f:
        CONFIG = yaml.safe_load(f)

    KAFKA_TOPICS = [topic for topic, settings in CONFIG.items() if settings['active']]

    kafka_process = start_kafka()
    kafka_admin = create_kafka_admin()
    producer = create_kafka_producer()
    try:     
        create_topics(kafka_admin, KAFKA_TOPICS)
        for topic in KAFKA_TOPICS:
            stream_starter(topic, CONFIG[topic], producer, test_mode= True)
    finally:
        producer.flush()
        kafka_admin.delete_topics(KAFKA_TOPICS)
        stop_kafka(kafka_process)

    return


main()