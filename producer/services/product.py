from random import choices, randrange, random
from typing import Annotated

from datetime import datetime, timedelta
import numpy as np

from injection import Injectable, inject
from dependencies.repositories import get_repository
from repository import ProductRepository
from services.base import BaseService


class ProductService(BaseService):
    @inject
    def create_product_category(
        self,
        product_repo: Annotated[ProductRepository, Injectable(get_repository(ProductRepository))]
    ) -> dict:
        cnt = 0
        while True:
            product_category = product_repo.create_product_category(
                name=self.fake.product_subcategory() if cnt > 0 else self.fake.product_category(),
                parent_category_id=product_category['category_id'] if cnt > 0 else None
            )
            self.send_to_kafka(
                topic="product_categories",
                key=str(product_category["category_id"]),
                obj=product_category
            )
            if cnt == 0 or random() > 0.5 and cnt < 4:
                cnt += 1
            else:
                break
        return product_category

    @inject
    def create_product(
        self,
        product_repo: Annotated[ProductRepository, Injectable(get_repository(ProductRepository))]
    ) -> dict:
        product_category = product_repo.get_product_category(
            category_id=randrange(1, 50)
        )
        if product_category is not None:
            category_id = product_category['category_id']
        else:
            category_id = self.create_product_category()['category_id']
        product = product_repo.create_product(
            name=' '.join([self.fake.product_adjective(), self.fake.product_material(), self.fake.product_name()]),
            description=self.fake.product_description(),
            category_id=category_id,
            price=round(np.random.lognormal(mean=1.0), 2),
            stock_quantity=randrange(1, 120),
            creation_date=self.fake.date_time_between(
                start_date=datetime(2020, 1, 1),
                end_date=datetime(2020, 2, 1)
            )
        )
        self.send_to_kafka(
            topic="products",
            key=str(product["product_id"]),
            obj=product
        )
        return product

    @inject
    def get_product(
        self,
        product_id: int,
        product_repo: Annotated[ProductRepository, Injectable(get_repository(ProductRepository))]
    ) -> dict:
        return product_repo.get_product(
            product_id=product_id
        )
