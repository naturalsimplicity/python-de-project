from random import choices, randrange, random
from typing import Annotated
from datetime import datetime, timedelta
import numpy as np

from ..injection import Injectable, inject
from ..dependencies.repositories import get_repository
from ..repository import OrderRepository, ProductRepository, UserRepository
from ..services.base import BaseService


class OrderService(BaseService):
    @inject
    def create_order(
        self,
        order_repo: Annotated[OrderRepository, Injectable(get_repository(OrderRepository))],
        product_repo: Annotated[ProductRepository, Injectable(get_repository(ProductRepository))],
        user_repo: Annotated[UserRepository, Injectable(get_repository(UserRepository))]
    ) -> dict:
        last_product_id = product_repo.get_last_product_id()
        last_user_id = user_repo.get_last_user_id()
        order_date = self.fake.date_time_between(
            start_date=datetime(2020, 1, 1),
            end_date=datetime.now()
        )
        order = order_repo.create_order(
            user_id=randrange(1, last_user_id),
            order_date=order_date,
            status=choices(
                population=['Aborted', 'Pending', 'Paid', 'Completed'],
                weights=[0.05, 0.1, 0.25, 0.6],
                k=1
            )[0],
            delivery_date=order_date + timedelta(seconds=randrange(10000, 1000000)),
            details=[
                {
                    'product_id': randrange(1, last_product_id),
                    'quantity': np.random.poisson(lam=1) + 1
                }
                for _ in range(np.random.poisson(lam=1) + 1)
            ]
        )
        self.send_to_kafka(
            topic="orders",
            key=str(order['order_id']),
            obj=order
        )
        return order
