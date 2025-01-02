from typing import Annotated
from confluent_kafka import Producer
from faker import Faker
import json

from ..injection import Injectable, inject
from ..dependencies.producers import get_producer
from ..samples.products import Provider as ProductProvider


class BaseService:
    @inject
    def __init__(
        self,
        producer: Annotated[Producer, Injectable(get_producer)]
    ):
        self.producer = producer
        self.fake = Faker()
        self.fake.add_provider(ProductProvider)

    def send_to_kafka(
        self,
        topic: str,
        key: str,
        obj: dict
    ):
        serialized_data = json.dumps(obj).encode('utf-8')
        self.producer.produce(
            topic=topic,
            key=key,
            value=serialized_data
        )
        self.producer.flush()
