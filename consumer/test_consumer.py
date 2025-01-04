from confluent_kafka import Consumer
import json

conf = {'bootstrap.servers': 'localhost:9092',
        'group.id': 'replication',
        'auto.offset.reset': 'smallest'}

consumer = Consumer(conf)


if __name__ == '__main__':
    try:
        consumer.subscribe(['orders'])

        while True:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue
            print(f"key={msg.key().decode('utf-8')}, value={msg.value().decode('utf-8')}")
    except KeyboardInterrupt:
        pass
    finally:
        # Close down consumer to commit final offsets.
        consumer.close()
