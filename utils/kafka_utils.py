import socket
import subprocess
import time
import os
from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient
from confluent_kafka.cimpl import NewTopic

KAFKA_LOCAL = {'bootstrap.servers': 'localhost:9092',
        'client.id': socket.gethostname()}

def acked(err, msg):
    if err is not None:
        print(f"Failed to deliver message: {err}")
    else:
        print(f"Topic: {msg.topic()} | Key: {msg.key()} | Value: {msg.value().decode('utf-8')}")

def create_kafka_producer():
    return Producer(KAFKA_LOCAL)

def create_kafka_admin() -> AdminClient:
    return AdminClient({'bootstrap.servers': KAFKA_LOCAL['bootstrap.servers']})
    
def create_topics(admin_client: AdminClient, topics: list[str]) -> None:
    '''
    Create topics (kafka tables) for the programs runtime.  
    '''
    current_topics = admin_client.list_topics().topics.keys()
    new_topics = [
        NewTopic(t, num_partitions=3, replication_factor=1) 
        for t in topics if t not in current_topics
    ]
    if new_topics:
        admin_client.create_topics(new_topics)


def start_kafka():
    kafka_dir = os.environ["KAFKA_DIR"]
    print("Starting Kafka")
    process = subprocess.Popen(
        [f"{kafka_dir}/bin/kafka-server-start.sh", 
         f"{kafka_dir}/config/server.properties"],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL
    )
    time.sleep(5)  # give Kafka time to start before producing
    return process

def stop_kafka(process):
    print("Stopping Kafka")
    process.terminate()
    process.wait()
