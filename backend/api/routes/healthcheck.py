import os
from fastapi import APIRouter
from starlette.requests import Request

from backend.models.healthcheck import HealthcheckResult
from backend.api.routes.metadata.endpoints import HEALTHCHECK_DESCRIPTION, MODEL_STATUS_DESCRIPTION

router = APIRouter()
CONNECT_TO_DB = os.getenv("CONNECT_TO_DB", False)


@router.get("/health", response_model=HealthcheckResult, name="heartbeat",
            description=HEALTHCHECK_DESCRIPTION)
async def get_hearbeat() -> HealthcheckResult:
    is_healthy = HealthcheckResult(is_alive=True)
    return is_healthy


# Model status endpoint
@router.get("/model_status", name="get_model_status",
            description=MODEL_STATUS_DESCRIPTION)
async def get_model_status(request: Request) -> str:
    model_name = request.app.state.model_name
    return f"Model was loaded: {model_name}."


@router.get("/db_status", name="get_db_status",
            description="Check if the database is available.",
            include_in_schema=CONNECT_TO_DB)
async def get_db_status(request: Request) -> str:
    try:
        db = await request.app.state.pool.acquire()
        await request.app.state.pool.release(db)
        return "Database is available."
    except Exception as e:
        return f"Database is not available: {e}"
