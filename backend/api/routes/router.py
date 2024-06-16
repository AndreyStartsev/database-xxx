import os
from fastapi import APIRouter
from loguru import logger

from backend.api.routes import healthcheck, inference, postgres_route
CONNECT_TO_DB = os.getenv("CONNECT_TO_DB", False)

api_router = APIRouter()
api_router.include_router(healthcheck.router, tags=["health"], prefix="")
api_router.include_router(inference.router, tags=["inference"], prefix="/model")

if os.getenv("CONNECT_TO_DB", False):
    logger.info("\033[92mConnecting to database...\033[0m")
    # api_router.include_router(oracle_route.router, tags=["oracle"], prefix="/oracle")
    api_router.include_router(postgres_route.router, tags=["postgres"], prefix="/postgres")
else:
    logger.info("\033[93mDatabase connection is disabled\033[0m")