from random import choices
from typing import Annotated

from datetime import datetime

from ..injection import Injectable, inject
from ..dependencies.repositories import get_repository
from ..repository import UserRepository
from ..services.base import BaseService


class UserService(BaseService):
    @inject
    def create_user(
        self,
        user_repo: Annotated[UserRepository, Injectable(get_repository(UserRepository))]
    ) -> dict:
        user = user_repo.create_user(
            first_name=self.fake.first_name(),
            last_name=self.fake.last_name(),
            email=self.fake.email(),
            phone_number=self.fake.country_calling_code() + self.fake.basic_phone_number(),
            registration_date=self.fake.date_time_between(
                start_date=datetime(2020, 1, 1),
                end_date=datetime.now()
            ),
            loyalty_status=choices(
                population=['Platinum', 'Gold', 'Silver', 'Basic'],
                weights=[0.05, 0.1, 0.25, 0.6],
                k=1
            )[0]
        )
        self.send_to_kafka(
            topic="items",
            key=str(user['user_id']),
            obj=user
        )
        return user

    @inject
    def get_user(
        self,
        user_id: int,
        user_repo: Annotated[UserRepository, Injectable(get_repository(UserRepository))]
    ):
        return user_repo.get_user(
            user_id=user_id
        )
