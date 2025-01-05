from sqlalchemy import create_engine, Connection
from typing import Callable, Annotated, Type

from injection import inject, Injectable
from repository import BaseRepository


engine = create_engine("postgresql+psycopg2://postgres_user:postgres_password@postgres-origin:5433/postgres_db")

conn = engine.connect()
def get_connection() -> Connection:
    return conn

def get_repository(
    repo_type: Type[BaseRepository],
) -> Callable[[Connection], BaseRepository]:
    @inject
    def _get_repository(
        connection: Annotated[Connection, Injectable(get_connection)]
    ) -> BaseRepository:
        return repo_type(connection)
    return _get_repository
