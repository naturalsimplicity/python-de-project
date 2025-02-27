from confluent_kafka import Producer
import socket


conf = {'bootstrap.servers': 'kafka:9093',
        'client.id': socket.gethostname()}
producer = Producer(conf)

def get_producer() -> Producer:
    return producer
