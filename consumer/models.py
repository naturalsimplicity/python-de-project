from sqlalchemy.orm import DeclarativeBase
from sqlalchemy.orm import Mapped, mapped_column
from sqlalchemy import String, TIMESTAMP, Integer, Numeric
from datetime import datetime
from typing import Optional
from decimal import Decimal


class Base(DeclarativeBase):
    def to_dict(self):
        obj = {}
        for column in self.__table__.columns:
            value = getattr(self, column.name)
            if isinstance(value, datetime):
                value = datetime.strftime(value, '%Y-%m-%d %H:%M:%S')
            elif isinstance(value, Decimal):
                value = float(value)
            obj.update({
                column.name: value
            })
        return obj

class User(Base):
    __tablename__ = "users"

    user_id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    first_name: Mapped[str] = mapped_column(String(255))
    last_name: Mapped[str] = mapped_column(String(255))
    email: Mapped[str] = mapped_column(String(255))
    phone_number: Mapped[str] = mapped_column(String(50))
    registration_date: Mapped[datetime] = mapped_column(TIMESTAMP)
    loyalty_status: Mapped[str] = mapped_column(String(50))

class Product(Base):
    __tablename__ = "products"

    product_id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    name: Mapped[str] = mapped_column(String(255))
    description: Mapped[str] = mapped_column(String(1000))
    category_id: Mapped[int] = mapped_column(Integer)
    price: Mapped[Decimal] = mapped_column(Numeric(19, 2))
    stock_quantity: Mapped[int] = mapped_column(Integer)
    creation_date: Mapped[datetime] = mapped_column(TIMESTAMP)

class ProductCategory(Base):
    __tablename__ = "product_categories"

    category_id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    name: Mapped[str] = mapped_column(String(255))
    parent_category_id: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)

class Order(Base):
    __tablename__ = "orders"

    order_id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    user_id: Mapped[int] = mapped_column(Integer)
    order_date: Mapped[datetime] = mapped_column(TIMESTAMP)
    total_amount: Mapped[Decimal] = mapped_column(Numeric(19, 2))
    status: Mapped[str] = mapped_column(String(255))
    delivery_date: Mapped[datetime] = mapped_column(TIMESTAMP)

class OrderDetails(Base):
    __tablename__ = "order_details"

    order_detail_id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    order_id: Mapped[int] = mapped_column(Integer)
    product_id: Mapped[int] = mapped_column(Integer)
    quantity: Mapped[int] = mapped_column(Integer)
    price_per_unit: Mapped[Decimal] = mapped_column(Numeric(19, 2))
    total_amount: Mapped[Decimal] = mapped_column(Numeric(19, 2))
