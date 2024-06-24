import time

import pandas as pd
import uuid
import json
import os
import io
from decimal import Decimal
from datetime import datetime
from fastapi import APIRouter, HTTPException, Depends, BackgroundTasks
from fastapi.responses import JSONResponse, FileResponse, StreamingResponse
from fastapi.concurrency import run_in_threadpool
from starlette.requests import Request
from loguru import logger
from concurrent.futures import ThreadPoolExecutor
from typing import List

from backend.core.db import connect_to_db_via_pool
from backend.core.security import validate_db_request
from backend.models.inference import ConfigNER, DatabaseDataPayload
from backend.api.adapters.postgres_adapters.connector import (
    AnonymizationParameters,
    PostgresqlConnector,
    AnalysisParameters
)

router = APIRouter()
executor = ThreadPoolExecutor()


@router.get("/connect_to_db", name="connect_to_db",
            description="Connect to the database.",
            include_in_schema=True)
async def connect_to_db(request: Request, postgres_connection_string: str = None):
    await connect_to_db_via_pool(request, postgres_connection_string)
    return JSONResponse(content={"message": "Connected to the database."}, status_code=200)


@router.get("/show_database_url", name="show_database_url",
            description="Show the database URL.",
            include_in_schema=True)
async def show_database_url(request: Request):
    return JSONResponse(content={"database_url": request.app.state.database}, status_code=200)


@router.get("/show_public_tables", name="show_public_tables",
            description="Show all public tables in the database.",
            include_in_schema=True)
async def show_public_tables(request: Request):
    try:
        async with request.app.state.pool.acquire() as connection:
            tables = await connection.fetch(
                "SELECT * FROM pg_catalog.pg_tables WHERE schemaname != 'pg_catalog' AND schemaname != 'information_schema'")
            if not tables:
                return JSONResponse(content={"error": "No tables found."}, status_code=404)
            jsonified_tables = []
            for table in tables:
                jsonified_tables.append({k: v for k, v in dict(table).items() if k in ['tablename', 'schemaname']})
                # add table column names
                table_name = table['tablename']
                schema_name = table['schemaname']
                columns = await connection.fetch(
                    f"SELECT column_name FROM information_schema.columns WHERE table_schema = '{schema_name}' AND table_name = '{table_name}'")
                jsonified_tables[-1]['columns'] = [dict(col)['column_name'] for col in columns]
            return JSONResponse(content={"tables": jsonified_tables}, status_code=200)
    except Exception as e:
        logger.error(f"Failed to retrieve tables: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/show_head", name="show_head",
            description="Show some entries from a table in the database.",
            include_in_schema=False)
async def show_head(request: Request, table_name: str, limit: int = 10):
    try:
        async with request.app.state.pool.acquire() as connection:
            table = await connection.fetch(f"SELECT * FROM {table_name} LIMIT {limit}")
            if not table:
                return JSONResponse(content={"error": "No table found."}, status_code=404)
            # convert asyncpg.Record to a JSON-serializable dictionary
            jsonified_table = [_serialize_record(row) for row in table]

            # check if it is json serializable
            for row in jsonified_table:
                for key, value in row.items():
                    try:
                        json.dumps(value)
                    except Exception as e:
                        row[key] = str(value)

            return JSONResponse(content={"table": jsonified_table}, status_code=200)
    except Exception as e:
        logger.error(f"Failed to retrieve table: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/test_postgres_connection", name="test_postgres_connection",
            description="Retrieve n random rows from Postgres DB.",
            include_in_schema=False,
            dependencies=[Depends(validate_db_request)])
async def test_postgres(request: Request,
                        table_name: str = "emr_history",
                        n: int = 10):
    try:
        rows = await request.app.state.pool.fetch(f"SELECT * FROM {table_name} LIMIT {n}")
        rows_dict = [_serialize_record(row) for row in rows]
        return JSONResponse(content={"postgres_data": rows_dict}, status_code=200)
    except Exception as e:
        logger.error(f"Failed to retrieve random sheet text: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/download_table", name="download_table",
            description="Download the entire table as a CSV file.",
            include_in_schema=False,
            dependencies=[Depends(validate_db_request)])
async def download_table(request: Request,
                         table_name: str = "emr_history",
                         limit: int = 100,
                         ):
    async with request.app.state.pool.acquire() as connection:
            connector = PostgresqlConnector(connection)
    data = await connector.get_entire_table_as_dataframe(table_name, limit=limit)

    buffer = io.StringIO()
    data.to_csv(buffer, index=False)
    buffer.seek(0)

    # Define type and headers for the response
    media_type = "text/csv"
    headers = {"Content-Disposition": f"attachment; filename={table_name}.csv"}
    return StreamingResponse(buffer, media_type=media_type, headers=headers)


@router.get("/get_table_row_count", name="get_table_row_count",
            description="Retrieve the row count of a specific table.",
            include_in_schema=False,
            dependencies=[Depends(validate_db_request)])
async def get_table_row_count(request: Request,
                              table_name: str = "emr_history"):
    try:
        row_count = await _get_row_count(request.app.state.pool, table_name)
        column_types = await _retrive_column_types(request.app.state.pool, table_name)

        return JSONResponse(content={"row_count": row_count, "column_types": column_types}, status_code=200)
    except Exception as e:
        logger.error(f"Failed to retrieve the row count: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/start-anonymization", name="start_anonymization",
             description="Start the anonymization process in the background.",
             include_in_schema=True,
             dependencies=[Depends(validate_db_request)])
async def start_anonymization(params: AnonymizationParameters,
                              background_tasks: BackgroundTasks,
                              request: Request, ):
    logger.info(f"Anonymization parameters: {params}")
    job_id = str(uuid.uuid4())
    background_tasks.add_task(anonymize_data_task,
                              params=params,
                              request=request,
                              job_id=job_id, )
    return {"message": "Anonymization started in the background.", "job_id": job_id}


@router.post("/start-analysis", name="start_analysis",
             description="Start the analyzing process in the background.",
             include_in_schema=True,
             dependencies=[Depends(validate_db_request)])
async def start_analysis(params: AnalysisParameters,
                         request: Request, ):
    logger.info(f"Analyzing parameters: \033[1m{params}\033[0m")
    job_id = str(uuid.uuid4())
    results = await analyze_data_task(params=params, request=request, job_id=job_id)
    # results = json.dumps(results, indent=4)
    return JSONResponse(content=results, status_code=200)


@router.get("/move_anonymized_tables", name="move_anonymized_tables",
            description="Move all anonymized tables to a separate schema.",
            include_in_schema=False,
            dependencies=[Depends(validate_db_request)])
async def move_anonymized_tables(request: Request, new_schema: str = "anonymized"):
    logger.info("Moving anonymized tables to a separate schema...")
    try:
        async with request.app.state.pool.acquire() as connection:
            connector = PostgresqlConnector(connection)
            await connector.move_tables_with_prefix(database_url=request.app.state.database, new_db=new_schema)
            logger.info("Anonymized tables moved to a separate schema.")
            return JSONResponse(content={"message": "Anonymized tables moved to a separate schema."}, status_code=200)
    except Exception as e:
        logger.error(f"Failed to move anonymized tables: {e}")
        raise HTTPException(status_code=500, detail=str(e))


def log_job(job_id, params, message=""):
    start = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    end = None
    if not os.path.exists(params.logfile):
        job_data = pd.DataFrame(columns=["job_id", "table_name", "column_name", "start", "end", "status", "message"])
    else:
        job_data = pd.read_csv(params.logfile)

    job_data = pd.concat([job_data, pd.DataFrame({
        "job_id": job_id,
        "table_name": f"{params.dest_table_prefix}_{params.src_table_name}",
        "column_name": "all of type text",
        "start": start,
        "end": end,
        "status": message}, index=[0])])

    job_data.to_csv(params.logfile, index=False)


def update_job_status(job_id, params, message):
    end = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    job_data = pd.read_csv(params.logfile)
    job_data.loc[job_data["job_id"] == job_id, "status"] = message
    job_data.loc[job_data["job_id"] == job_id, "end"] = end
    job_data.to_csv(params.logfile, index=False)


async def anonymize_data_task(params: AnonymizationParameters, request: Request = None, job_id: str = None):
    logger.info(f"Starting anonymization task with job ID: {job_id}")
    log_job(job_id, params, message="STARTED")
    limit = None if params.entries_limit == 0 else params.entries_limit  # Set limit to None/"all" if 0

    # Set up the NER config
    config = ConfigNER()
    config.aggressive = True
    config.remove_html = True

    # Connect to the database and start the anonymization process
    async with request.app.state.pool.acquire() as connection:
        connector = PostgresqlConnector(connection)
        data_stream = connector.stream_data(params.src_table_name, chunk_size=100, limit=limit)

        if params.dest_type == 'db':
            await connector.anonymize_data_to_db(params, request, job_id, data_stream, config)
        elif params.dest_type == 'csv':
            await connector.anonymize_data_to_csv(params, request, job_id, data_stream, config)

    update_job_status(job_id, params=params, message="FINISHED")


async def analyze_data_task(params: AnalysisParameters, request: Request = None, job_id: str = None):
    logger.info(f"Starting analyzing task with job ID: {job_id}")
    log_job(job_id, params, message="STARTED")
    profiled_data = {}

    async with request.app.state.pool.acquire() as connection:
        connector = PostgresqlConnector(connection)
        logger.info("Connector created.")
        tables = await connector.return_table_names()
        logger.info(f"Tables found: {tables}")
        if not tables:
            return {"error": "No tables found."}

        for table in tables:
            data_stream = connector.stream_data(table, chunk_size=1000)
            params.src_table_name = table
            result = await connector.profile_table(params, request, job_id, data_stream)
            logger.info(f"Analysis result: {result}")
            profiled_data[table] = result

    # Save the result to a file
    result_file = f"{params.result_folder}{params.src_table_name}_analysis.log"
    result_str = json.dumps(profiled_data, indent=4)
    with open(result_file, "w") as f:
        f.write(result_str)

    update_job_status(job_id, params=params, message="FINISHED")
    return profiled_data


def _serialize_record(record):
    """
    - convert asyncpg.Record to a JSON-serializable dictionary
    - convert Decimal to float
    - convert datetime to ISO string
    - convert bytes to string
    - convert UUID to string
    """
    result = {}
    if isinstance(record, list):
        return [_serialize_record(rec) for rec in record]
    for key, value in record.items():
        if isinstance(value, Decimal):
            result[key] = float(value)
        elif isinstance(value, datetime):
            result[key] = value.isoformat()
        elif isinstance(value, bytes):
            result[key] = value.decode()
        elif isinstance(value, uuid.UUID):
            result[key] = str(value)
        elif isinstance(value, set):
            result[key] = list(value)
        else:
            result[key] = value
    return result


async def _retrive_column_types(connection, table_name):
    column_types = await connection.fetch(f"""
            SELECT column_name, data_type
            FROM information_schema.columns
            WHERE table_schema = 'public' AND table_name = '{table_name}'
        """)
    column_types = [{"column_name": col["column_name"], "data_type": col["data_type"]} for col in column_types]
    return column_types


async def _get_row_count(connection, table_name):
    row_count = await connection.fetchval(f"SELECT COUNT(*) FROM {table_name}")
    return row_count
