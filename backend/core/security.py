import secrets
from typing import Optional

from fastapi import HTTPException, Security, Depends
from fastapi.security.api_key import APIKeyHeader
from starlette.status import HTTP_400_BAD_REQUEST, HTTP_401_UNAUTHORIZED

from backend.core import config
from backend.core.messages import AUTH_REQ, NO_API_KEY

api_key = APIKeyHeader(name="xxx", auto_error=False)


def validate_request(header: Optional[str] = Security(api_key)) -> bool:
    if header is None:
        raise HTTPException(
            status_code=HTTP_400_BAD_REQUEST, detail=NO_API_KEY, headers={}
        )
    if not secrets.compare_digest(header, str(config.API_KEY_USER)) and \
            not secrets.compare_digest(header, str(config.API_KEY_APP)):
        raise HTTPException(
            status_code=HTTP_401_UNAUTHORIZED, detail=AUTH_REQ, headers={}
        )
    return True


def validate_db_request(header: Optional[str] = Security(api_key)) -> bool:
    if header is None:
        raise HTTPException(
            status_code=HTTP_400_BAD_REQUEST, detail=NO_API_KEY, headers={}
        )
    if not secrets.compare_digest(header, str(config.API_DB_KEY)):
        raise HTTPException(
            status_code=HTTP_401_UNAUTHORIZED, detail=AUTH_REQ, headers={}
        )
    return True


def identify_user_id(header: str = Depends(api_key)):
    if header == str(config.API_KEY_APP):
        return "admin"
    else:
        return "user"