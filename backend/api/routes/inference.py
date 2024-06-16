import os.path
import datetime
from fastapi import APIRouter, Depends, Query, UploadFile, File
from fastapi.exceptions import HTTPException
from fastapi.responses import HTMLResponse, JSONResponse
from starlette.requests import Request
from pydantic import BaseModel, Field
from typing import List, Union
from loguru import logger

from backend.core.security import validate_request
from backend.models.inference import ConfigNER, BatchInput, NameList
from backend.services.ml_model import SpacyModel
from backend.api.routes.metadata.endpoints import ANONIMIZATION_DESCRIPTION

router = APIRouter()

BATCH_LEN_LIMIT = 10
USER_REQUEST_LIMIT = 1000
logs_dir = os.path.join(os.getenv("ROOT", "./backend"), "../logs")
REQUESTS_FILE_PATH = os.path.join(logs_dir, "user_requests.log")


@router.post("/anonymize", name="anonymize_batch",
             description=ANONIMIZATION_DESCRIPTION,
             include_in_schema=False,
             dependencies=[Depends(validate_request)],
             )
async def anonymize(
        request: Request,
        batch_input: BatchInput,
        names_list: NameList = None,
        config: ConfigNER = Depends(),

):
    is_admin = request.state.user_id == "admin"
    logger.info(f"\033[1;32;40mRequest user id: {request.state.user_id}\033[0m")

    model: SpacyModel = request.app.state.model
    use_rules = config.aggressive
    placeholder = config.placeholder
    ents_to_hide = config.entities_to_hide
    fuzzy_match = config.fuzzy_match
    per_list_label = config.per_list_label
    remove_html = config.remove_html
    names = None

    if not is_admin:
        # Validate batch length
        if not _validate_batch_len(len(batch_input.texts)):
            logger.error(f"Batch length is exceeded.")
            raise HTTPException(status_code=400,
                                detail=f"Batch length is limited to {BATCH_LEN_LIMIT} texts. "
                                       f"Please provide a batch of max {BATCH_LEN_LIMIT} texts.")
        # Validate user request number
        if not _validate_user_request_num():
            logger.error(f"User request limit is exceeded.")
            raise HTTPException(status_code=402,
                                detail=f"Your free limit of {USER_REQUEST_LIMIT} requests is exceeded. "
                                       f"Please contact admin to increase the limit.")

    if names_list is not None:
        names = names_list.names

    logger.debug(f"\033[093mReceived names: {names}\033[090m | Names list: {names_list}\033[0m")

    try:
        predictions = [model.predict(payload=text,
                                     use_rules=use_rules,
                                     use_base_model=False,
                                     placeholder=placeholder,
                                     ents_to_hide=ents_to_hide,
                                     checklist=names,
                                     fuzzy_match=fuzzy_match,
                                     per_list_label=per_list_label,
                                     remove_html=remove_html,)
                       for text in batch_input.texts]

        logger.info(f"\033[090mReceived batch of texts: {batch_input.texts}\033[0m")
        logger.info(f"\033[096mPredictions: {[p['personal_data'] for p in predictions]}\033[0m")

        if not is_admin:
            # Save user request
            _save_user_request({"texts": batch_input.texts, "predictions": predictions, "rules": use_rules,
                                "exclude_names": names, "fuzzy_match": fuzzy_match})

        return JSONResponse(content={"predictions": predictions}, status_code=200)

    except Exception as e:
        logger.error(f"Failed to process batch: {e}")
        raise HTTPException(status_code=500, detail=str(e))


def _validate_entities_list(entities_list: List[str]):
    # Validate entities list
    valid_entities = ["PER", "LOC", "ORG", "DATE", "CONTACTS", "SENSITIVE"]
    validated_entities = []
    for ent in entities_list:
        if ent in valid_entities:
            validated_entities.append(ent)
    return validated_entities


def _validate_batch_len(batch_len: int):
    if batch_len >= BATCH_LEN_LIMIT:
        return False
    return True


def _validate_user_request_num(file_path=REQUESTS_FILE_PATH):
    if not os.path.exists(file_path):
        return True
    with open(file_path, "r") as f:
        lines = f.readlines()
    if len(lines) >= USER_REQUEST_LIMIT:
        return False
    return True


def _save_user_request(request: dict, file_path=REQUESTS_FILE_PATH):
    try:
        request["timestamp"] = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        request["predictions"] = [p['personal_data'] for p in request["predictions"]]
        # request["texts"] = [t.data for t in request["texts"]]
        file_read_mode = "a" if os.path.exists(file_path) else "w"
        with open(file_path, file_read_mode) as f:
            f.write(str(request) + "\n")
    except Exception as e:
        logger.error(f"Failed to save user request: {e} | {request}")


async def process_uploaded_file(file: UploadFile) -> List[str]:
    # Validate file type
    if not file.content_type.startswith('text/plain'):
        raise HTTPException(status_code=400, detail="Invalid file type. Please upload a text file.")

    # Read and process file content
    file_content = await file.read()
    names = file_content.decode('utf-8').strip().split('\n')
    return names
