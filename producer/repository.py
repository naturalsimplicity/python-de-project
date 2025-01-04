from faker import Faker
from datetime import datetime, timedelta
from decimal import Decimal

from sqlalchemy.orm import Session, sessionmaker
from sqlalchemy import select, insert, Connection, func

from samples.products import Provider as ProductProvider
from models import User, Product, ProductCategory, Order, OrderDetails

fake = Faker(['en_PH'])
fake.add_provider(ProductProvider)


class BaseRepository:
    def __init__(
        self,
        connection: Connection
    ):
        self.connection = connection

class UserRepository(BaseRepository):
    def create_user(
        self,
        first_name: str,
        last_name: str,
        email: str,
        phone_number: str,
        registration_date: datetime,
        loyalty_status: str
    ) -> dict:
        with Session(self.connection) as session:
            user = session.execute(
                insert(User)
                .values(
                    first_name=first_name,
                    last_name=last_name,
                    email=email,
                    phone_number=phone_number,
                    registration_date=registration_date,
                    loyalty_status=loyalty_status
                )
                .returning(User)
            ).scalar()
            session.commit()
            return user.to_dict()

    def get_last_user_id(
        self
    ) -> int:
        with Session(self.connection) as session:
            return session.execute(
                func.max(User.user_id)
            ).scalar_one()

class ProductRepository(BaseRepository):
    def get_product(
        self,
        product_id: int
    ) -> dict:
        with Session(self.connection) as session:
            product = session.execute(
                select(Product)
                .where(Product.product_id == product_id)
            ).scalar()
            return product.to_dict()

    def create_product(
        self,
        name: str,
        description: str,
        category_id: int,
        price: float,
        stock_quantity: int,
        creation_date: datetime
    ) -> dict:
        with Session(self.connection) as session:
            product = session.execute(
                insert(Product)
                .values(
                    name=name,
                    description=description,
                    category_id=category_id,
                    price=price,
                    stock_quantity=stock_quantity,
                    creation_date=creation_date
                )
                .returning(Product)
            ).scalar()
            session.commit()
            return product.to_dict()

    def create_product_category(
        self,
        name: str,
        parent_category_id: int
    ) -> dict:
        with Session(self.connection) as session:
            product_category = session.execute(
                insert(ProductCategory)
                .values(
                    name=name,
                    parent_category_id=parent_category_id
                )
                .returning(ProductCategory)
            ).scalar()
            session.commit()
            return product_category.to_dict()

    def get_product_category(
        self,
        category_id: int
    ) -> dict:
        with Session(self.connection) as session:  # expire_on_commit = False
            product_category = session.execute(
                select(ProductCategory)
                .where(ProductCategory.category_id == category_id)
            ).scalar()
            return product_category.to_dict() if product_category else None

    def get_last_product_id(
        self
    ) -> int:
        with Session(self.connection) as session:
            return session.execute(
                func.max(Product.product_id)
            ).scalar_one()

class OrderRepository(BaseRepository):
    def get_last_order_id(
        self
    ) -> int:
        with Session(self.connection) as session:
            return session.execute(
                func.max(Order.order_id)
            ).scalar_one()

    def get_price_per_unit(
        self,
        product_id: int
    ) -> Decimal:
        with Session(self.connection) as session:
            return session.execute(
                select(Product.price)
                .where(Product.product_id == product_id)
            ).scalar_one()

    def create_order(
        self,
        user_id: int,
        order_date: datetime,
        status: str,
        delivery_date: datetime,
        details: list[dict]
    ) -> dict:
        with Session(self.connection) as session:
            order_id = self.get_last_order_id()
            if order_id is None:
                order_id = 1
            else:
                order_id += 1

            order_details = []
            for detail in details:
                price_per_unit = self.get_price_per_unit(detail['product_id'])
                order_detail = session.execute(
                    insert(OrderDetails)
                    .values(
                        order_id=order_id,
                        product_id=detail['product_id'],
                        quantity=detail['quantity'],
                        price_per_unit=price_per_unit,
                        total_amount=price_per_unit * detail['quantity']
                    )
                    .returning(OrderDetails)
                ).scalar()
                order_details.append(order_detail.to_dict())

            order = session.execute(
                insert(Order)
                .values(
                    user_id=user_id,
                    order_date=order_date,
                    total_amount=sum([detail['total_amount'] for detail in order_details]),
                    status=status,
                    delivery_date=delivery_date
                )
                .returning(Order)
            ).scalar()
            session.commit()
            obj = order.to_dict()
            obj['order_details'] = order_details
            return obj
