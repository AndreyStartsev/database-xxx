import os
from typing import Callable
from fastapi import FastAPI
from loguru import logger

from backend.services.ml_model import SpacyModel, TestModel
# from backend.services.pd_generator import PersonalDataGenerator
from backend.core.db import connect_to_db, close_db_connection

CONNECT_TO_DB = os.getenv("CONNECT_TO_DB", "True").lower() in ("true", "1")
logger.info(f"CONNECT_TO_DB: {CONNECT_TO_DB}")


async def _startup_model(app: FastAPI) -> None:
    # Load the model
    try:
        logger.info("Attempting to load model.")
        app.state.model = SpacyModel()
    except Exception as e:
        logger.error(f"Failed to load model: {e}")
        app.state.model = TestModel()

    app.state.model_name = app.state.model.model_name


async def _shutdown_model(app: FastAPI) -> None:
    logger.info("Running app shutdown handler.")
    # Unload the model
    app.state.model = None


def start_app_handler(app: FastAPI) -> Callable:
    async def startup() -> None:
        logger.info("Running app start handler.")
        await _startup_model(app)
        if CONNECT_TO_DB:
            await connect_to_db(app)
            logger.info("Connected to database.")

    return startup


def stop_app_handler(app: FastAPI) -> Callable:
    async def shutdown() -> None:
        logger.info("Running app shutdown handler.")
        await _shutdown_model(app)
        if CONNECT_TO_DB:
            await close_db_connection(app)

    return shutdown
