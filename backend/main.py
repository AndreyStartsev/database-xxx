# uvicorn backend.main:app --reload --port 8001 --env-file .env.dev
import os
import sys
from loguru import logger
from fastapi import FastAPI, Request, Response
from fastapi.staticfiles import StaticFiles

from backend.core.security import identify_user_id
from backend.api.routes.router import api_router
from backend.api.routes.html_route import router as html_router
from backend.core.config import (API_PREFIX, APP_NAME, APP_VERSION, DEBUG)
from backend.core.event_handlers import (start_app_handler, stop_app_handler)

ROOT = os.getenv("ROOT", "./backend")
LOGFILE = os.getenv("LOGFILE", "logfile.json")
logger.remove()
logger.add(sys.stderr, format="{time} {level} {message}", level="INFO")
logger.add(LOGFILE, rotation="500 MB", level="INFO", serialize=True)


async def extract_user_id_middleware(request: Request, call_next):
    api_key = request.headers.get('xxx')
    user_id = identify_user_id(api_key)
    request.state.user_id = user_id
    response = await call_next(request)
    return response


async def log_requests(request: Request, call_next):
    if not (request.method == "GET" and request.url.path == "/api/health"):
        logger.info(f"Request: {request.method} {request.url}")
    response: Response = await call_next(request)
    if not (request.method == "GET" and request.url.path == "/api/health"):
        logger.info(f"Response: {response.status_code}")
    return response


def get_app() -> FastAPI:
    fast_app = FastAPI(title=APP_NAME, version=APP_VERSION, debug=DEBUG)
    fast_app.include_router(api_router, prefix=API_PREFIX)
    fast_app.include_router(html_router)

    fast_app.add_event_handler("startup", start_app_handler(fast_app))
    fast_app.add_event_handler("shutdown", stop_app_handler(fast_app))

    # add static files
    static_path = os.path.join(ROOT, "static")
    fast_app.mount("/static", StaticFiles(directory=static_path), name="static")

    # add middleware
    fast_app.middleware("http")(extract_user_id_middleware)
    fast_app.middleware("http")(log_requests)

    return fast_app


app = get_app()
logger.info("App is running")
