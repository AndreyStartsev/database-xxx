import os
import asyncpg
from fastapi import FastAPI, Request
from loguru import logger
from sqlalchemy.dialects import postgresql

# Database Connection Settings
DB_USER = os.getenv("DB_USER", "postgres")
DB_PASSWORD = os.getenv("DB_PASSWORD", "postgres")
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_NAME = os.getenv("DB_NAME", "test")

# Database Connection URL
DATABASE_URL = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"


async def connect_to_db(app: FastAPI) -> None:
    logger.info("Connecting to PostgreSQL")

    app.state.pool = await asyncpg.create_pool(
        str(DATABASE_URL),
        min_size=10,
        max_size=10,
    )
    app.state.database = DATABASE_URL

    logger.info("Connection established")


async def connect_to_db_via_pool(request: Request, postgres_connection_string: str) -> None:
    if not postgres_connection_string:
        raise ValueError("Postgres connection string is required")

    logger.info("Connecting to PostgreSQL via pool")

    request.app.state.pool = await asyncpg.create_pool(
        str(postgres_connection_string),
        min_size=10,
        max_size=10,
    )
    request.app.state.database = postgres_connection_string

    logger.info("Connection established")


async def close_db_connection(app: FastAPI) -> None:
    logger.info("Closing connection to database")

    await app.state.pool.close()

    logger.info("Connection closed")


async def get_db(request: Request) -> asyncpg.Pool:
    return request.app.state.pool


async def _compile(query) -> str:
    compiled_query = query.compile(dialect=postgresql.asyncpg.dialect(),
                                   compile_kwargs={"literal_binds": True}
                                   )

    logger.debug(str(compiled_query))

    return str(compiled_query)
