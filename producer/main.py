from time import sleep

from .models import Base
from .services.order import OrderService
from .services.user import UserService
from .services.product import ProductService


if __name__ == '__main__':
    from .dependencies.repositories import engine
    Base.metadata.create_all(engine)

    user_service = UserService()
    for _ in range(1000):
        user = user_service.create_user()
        print(user)

    product_service = ProductService()
    for _ in range(100):
        product = product_service.create_product()
        print(product)

    order_service = OrderService()
    try:
        while True:
            order = order_service.create_order()
            print(order)
            sleep(1)
    except KeyboardInterrupt:
        pass
