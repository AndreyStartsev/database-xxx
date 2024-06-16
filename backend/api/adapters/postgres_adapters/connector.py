import os
import pandas as pd
from fastapi import HTTPException, Request
from asyncpg import Connection
from loguru import logger
from datetime import datetime
from typing import AsyncIterable, List, Union, AsyncGenerator
from pydantic import BaseModel, Field

from backend.models.inference import ConfigNER, DatabaseDataPayload

DUPLICATE_TABLE_SUFFIX = "date"  # suffix to add to the table name if it already exists: "date" or "null"

DATA_MAPPING = {
    "text": str,
    "character varying": str,
    "date": str,
    "integer": int,
    "float": float,
    "int": int,
    "numeric": float,
}


class AnonymizationParameters(BaseModel):
    entries_limit: Union[int, None] = Field(default=None, description="The number of entries to anonymize.")
    src_table_name: str = Field(default="users", description="The name of the table to anonymize.")
    dest_table_prefix: str = Field(default="anonymized", description="A prefix for table with anonymized data.")
    dest_type: str = Field(default="csv", description="The type of the destination file (csv or db).")
    dest_csv_file_folder: str = Field(default="/code/logs/", description="The folder to save the source CSV file.")
    logfile: str = Field(default="/code/logs/anonymization_jobs.log", description="The name of the logfile to use.")
    drop_existing_table: bool = Field(default=True, description="Whether to drop the existing table.")
    columns: List[str] = Field(default=None, description="The columns to anonymize.")
    strategy_by_column: dict = Field(default=None, description="The anonymization strategy by column.")


class AnalysisParameters(BaseModel):
    src_table_name: str = Field(default="users", description="The name of the table to analyze.")
    analysis_type: str = Field(default="column", description="The type of analysis to perform. (column, row)")
    logfile: str = Field(default="/code/logs/analysis_jobs.log", description="The name of the logfile to use.")
    dest_table_prefix: str = Field(default="", description="A prefix for table with analysis data.")
    result_folder: str = Field(default="/code/logs/", description="The folder to save the profile CSV file.")


async def write_to_csv(anonymized_data, csv_file):
    anonymized_data.to_csv(csv_file, mode="a", header=False, index=False)


async def process_and_anonymize_chunk(chunk, columns, request, config, include_columns=None,
                                      strategy_by_column=None):
    # if column type is not in the list, it will be skipped
    DEFAULT_STRATEGY_BY_TYPE = {"text": "model",
                                "character varying": "model",
                                "date": "date_generator",
                                "integer": "number_generator",
                                "float": "number_generator",
                                "int": "number_generator",
                                "numeric": "number_generator",
                                }

    MAPPING_STRATEGY_BY_COLUMN = {
        "PER": "name_generator",
        "LOC": "location_generator",
        "ORG": "organization_generator",
        "DATE": "date_generator",
        "SENSITIVE": "number_generator",
        "CONTACTS": "number_generator",
        "EMAIL": "email_generator",
        "PHONE": "phone_generator",
        "URL": "url_generator",
        "TEXT": "model",
    }

    if not isinstance(columns, list):
        columns = [{"column_name": columns, "data_type": "text"}]

    logger.info(f"\033[093mProcessing columns: {columns}\033[0m")
    logger.info(f"\033[093mChunk [1st element]: {chunk[0]}\033[0m")

    # convert chunk to DataFrame
    chunk = pd.DataFrame(chunk, columns=[col["column_name"] for col in columns])
    logger.info(f"\033[096mColumns in chunk: {chunk.columns}\033[0m")

    for column in columns:
        column_name = column["column_name"]
        data_type = column["data_type"]

        if include_columns and column_name not in include_columns:
            logger.info(f"\033[090m[EXCLUDED] Skipping column '{column_name}' - not in {include_columns}\033[0m")
            continue

        if data_type not in DEFAULT_STRATEGY_BY_TYPE:
            logger.info(
                f"\033[090m[TYPE MISMATCH] Skipping column '{column_name}' with data type '{data_type}'\033[0m")
            continue

        if column_name not in chunk.columns:
            logger.info(f"\033[090m[NO DATA] Column '{column_name}' not found in the chunk\033[0m")
            continue

        logger.info(f"\033[093mAnonymizing column '{column_name} ({data_type})'...\033[0m")
        anonymization_type = DEFAULT_STRATEGY_BY_TYPE[data_type]

        # Check if column has a specific anonymization strategy
        if strategy_by_column and column_name in strategy_by_column:
            anonymization_type_cat = strategy_by_column[column_name]
            anonymization_type = MAPPING_STRATEGY_BY_COLUMN.get(anonymization_type_cat, anonymization_type)

        logger.info(f"\033[096mAnonymization type: {anonymization_type}\033[0m")

        if anonymization_type == "model":
            batch = [DatabaseDataPayload(data=str(text)) for text in chunk[column_name].tolist()]
            predictions = request.app.state.model.predict_batch(
                batch=batch,
                use_rules=config.aggressive,
                placeholder=config.placeholder,
                ents_to_hide=config.entities_to_hide,
                fuzzy_match=config.fuzzy_match,
                per_list_label=config.per_list_label,
                remove_html=config.remove_html
            )
            if len(predictions["text"]) != len(chunk):
                logger.error(
                    f"Failed to anonymize chunk, predictions: {len(predictions['text'])} | chunk: {len(chunk)}")
                logger.info(f"\033[093m{type(predictions)}\033[0m, {predictions.keys()}")
                logger.info(f"\033[090m{predictions['text']}\033[0m")
            else:
                logger.info(f"\033[092mChunk anonymized successfully ✅\033[0m")
                chunk[column_name] = predictions["text"]

        elif anonymization_type == "date_generator":
            # left nans as is
            # chunk[column_name] = [request.app.state.model.pd_generator.generate(str(d), 'DATE')
            #                       for d in range(len(chunk)) ]
            chunk[column_name] = [request.app.state.model.pd_generator.generate("today", 'DATE')
                                  if pd.isna(d) or d is pd.NaT
                                  else request.app.state.model.pd_generator.generate(str(d), 'DATE')
                                  for d in chunk[column_name]]
        elif anonymization_type == "number_generator":
            chunk[column_name] = [request.app.state.model.pd_generator.generate(str(d), 'SENSITIVE')
                                  if d is not pd.isna(d)
                                  else request.app.state.model.pd_generator.generate("nan", 'SENSITIVE')
                                  for d in range(len(chunk))]

        elif anonymization_type == "name_generator":
            chunk[column_name] = [request.app.state.model.pd_generator.generate(str(d), 'PER')
                                  for d in range(len(chunk))]

        elif anonymization_type == "location_generator":
            chunk[column_name] = [request.app.state.model.pd_generator.generate(str(d), 'LOC')
                                  for d in range(len(chunk))]

        elif anonymization_type == "organization_generator":
            chunk[column_name] = [request.app.state.model.pd_generator.generate(str(d), 'ORG')
                                  for d in range(len(chunk))]

        elif anonymization_type == "email_generator":
            chunk[column_name] = [request.app.state.model.pd_generator.generate(str(d), 'EMAIL')
                                  for d in range(len(chunk))]

        elif anonymization_type == "phone_generator":
            chunk[column_name] = [request.app.state.model.pd_generator.generate(str(d), 'PHONE')
                                  for d in range(len(chunk))]

        elif anonymization_type == "url_generator":
            chunk[column_name] = [request.app.state.model.pd_generator.generate(str(d), 'URL')
                                  for d in range(len(chunk))]

        # if strategy by column was used - convert data to required type (e.g. int, float, str)
        if strategy_by_column and column_name in strategy_by_column:
            pd_type = DATA_MAPPING.get(data_type, str)
            chunk[column_name] = chunk[column_name].astype(pd_type)

            if data_type == 'date':
                chunk[column_name] = pd.to_datetime(chunk[column_name], format='%d.%m.%Y')

    logger.info(f"\033[096mColumns in chunk after anonymization: {chunk.columns}\033[0m")
    return chunk


async def anonymize_concatenated_text(text, request, config):
    payload = DatabaseDataPayload(data=text)
    predictions = request.app.state.model.predict(
        payload,
        use_rules=config.aggressive,
        placeholder=config.placeholder,
        ents_to_hide=config.entities_to_hide,
        fuzzy_match=config.fuzzy_match,
        per_list_label=config.per_list_label,
        remove_html=config.remove_html
    )
    return predictions


class PostgresqlConnector:
    def __init__(self, connection):
        self.connection = connection

    async def return_table_names(self):
        query = "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'"
        result = await self.connection.fetch(query)
        return [table["table_name"] for table in result]

    async def table_exists(self, table_name: str) -> bool:
        query = f"SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = '{table_name}')"
        result = await self.connection.fetchval(query)
        return result

    async def create_table_with_same_structure(self, src_table_name: str, dest_table_name: str):
        query = f"SELECT column_name, data_type FROM information_schema.columns WHERE table_name = '{src_table_name}'"
        columns = await self.connection.fetch(query)

        if not columns:
            raise HTTPException(status_code=404, detail=f"Source table {src_table_name} not found")

        columns_definition = ", ".join([f'"{col["column_name"]}" {col["data_type"]}' for col in columns])
        create_table_query = f"CREATE TABLE {dest_table_name} ({columns_definition})"

        logger.info(f"\033[090mCreating table '{dest_table_name}' with columns: {columns_definition}\033[0m")
        logger.info(f"\033[093mCreate table query: {create_table_query}\033[0m")

        await self.connection.execute(create_table_query)

    async def insert_data_to_table(self, table_name: str, data: pd.DataFrame):
        def _replace_nan(vals, default_date="nan", default_int=0):
            today = datetime.now().strftime("%Y-%m-%d")
            default_timestamp = pd.Timestamp(today) if default_date == "today" \
                else None if default_date == "nan" \
                else pd.Timestamp(default_date)
            vals = [
                tuple(
                    default_int if (pd.isna(value) and isinstance(value, (int, float))) else
                    default_timestamp if (pd.isna(value) and isinstance(value, pd.Timestamp)) else
                    default_timestamp if (value is pd.NaT) else
                    value
                    for value in row
                )
                for row in vals
            ]
            return vals

        def convert_types(row):
            return tuple(
                int(value) if isinstance(value, str) and value.isdigit() else
                float(value) if isinstance(value, str) and value.replace('.', '', 1).isdigit() else
                value
                for value in row
            )

        data = data.dropna(axis=1, how='all')
        data = data.where(pd.notnull(data), None)

        columns = data.columns.tolist()
        logger.info(f"\033[090mInserting data to table '{table_name}' with columns: {columns}\033[0m")

        columns = [col.strip('"') for col in columns]
        columns_str = ", ".join([f'"{col}"' for col in columns])
        values_str = ", ".join([f"${i + 1}" for i in range(len(columns))])

        query = f"INSERT INTO {table_name} ({columns_str}) VALUES ({values_str})"
        values = [tuple(row) for row in data.values]
        values = _replace_nan(values)
        # values = [convert_types(value) for value in values]

        for value in values:
            await self.connection.execute(query, *value)

    async def anonymize_data_to_csv(self, params: AnonymizationParameters, request: Request, job_id: str,
                                    data_stream: AsyncIterable[list], config: ConfigNER):
        anonymized_data = pd.DataFrame()
        csv_file = params.dest_csv_file_folder + f"test-postgres-{job_id}.csv"
        csv_file_tmp = params.dest_csv_file_folder + f"test-postgres-{job_id}-tmp.csv"

        try:
            idx = 0
            columns_and_types = await self._retrive_column_types(params.src_table_name)
            async for chunk in data_stream:
                logger.info(f"Processing chunk {idx}...")
                update_job_status(job_id, params=params, message=f"CURRENT CHUNK: {idx}")
                chunk = await process_and_anonymize_chunk(chunk, columns_and_types, request, config,
                                                          include_columns=params.columns,
                                                          strategy_by_column=params.strategy_by_column)
                anonymized_data = pd.concat([anonymized_data, chunk], ignore_index=True)
                await write_to_csv(chunk, csv_file_tmp)
                idx += 1
            await write_to_csv(anonymized_data, csv_file)

        except Exception as e:
            logger.error(f"Failed to anonymize data: {e}")
            update_job_status(job_id, params=params, message=f"FAILED: {e}")
            raise HTTPException(status_code=500, detail=str(e))

        logger.info(f"\033[092mAnonymized data saved to file: {csv_file}\033[0m")

    async def anonymize_data_to_db(self, params: AnonymizationParameters, request: Request, job_id: str,
                                   data_stream: AsyncIterable[list], config: ConfigNER):
        logger.info(f"\033[093mAnonymization parameters: {params}\033[0m")
        dest_table_name = f"{params.dest_table_prefix}_{params.src_table_name}"
        try:
            async with request.app.state.pool.acquire() as connection:
                if await self.table_exists(dest_table_name):
                    if params.drop_existing_table:
                        await connection.execute(f"DROP TABLE {dest_table_name}")
                        await self.create_table_with_same_structure(params.src_table_name, dest_table_name)
                    elif DUPLICATE_TABLE_SUFFIX == "date":
                        date_suffix = datetime.now().strftime("%m-%d-%y")
                        dest_table_name = f"{dest_table_name}_{date_suffix}"
                        await self.create_table_with_same_structure(params.src_table_name, dest_table_name)
                    elif DUPLICATE_TABLE_SUFFIX == "null":
                        raise HTTPException(status_code=409, detail=f"Table {dest_table_name} already exists")
                else:
                    await self.create_table_with_same_structure(params.src_table_name, dest_table_name)

                idx = 0
                columns_and_types = await self._retrive_column_types(params.src_table_name)
                async for chunk in data_stream:
                    logger.info(f"Processing chunk {idx}...")
                    update_job_status(job_id, params=params, message=f"CURRENT CHUNK: {idx}")
                    anonymized_chunk = await process_and_anonymize_chunk(chunk, columns_and_types, request, config,
                                                                         include_columns=params.columns,
                                                                         strategy_by_column=params.strategy_by_column)

                    # Define columns which will be processed
                    anonymized_chunk.columns = [f'"{col}"' for col in anonymized_chunk.columns]

                    await self.insert_data_to_table(dest_table_name, anonymized_chunk)
                    idx += 1

            update_job_status(job_id, params=params, message="FINISHED")

        except Exception as e:
            logger.error(f"Failed to anonymize data: {e}")
            update_job_status(job_id, params=params, message=f"FAILED: {e}")
            raise HTTPException(status_code=500, detail=str(e))

        logger.info(f"\033[092mAnonymized data saved to table: {dest_table_name}\033[0m")

    async def profile_table(self, params: AnalysisParameters, request: Request, job_id: str,
                            data_stream: AsyncIterable[list]):
        """
        Profile the data, that is for each column
        - if it is of type text, concatenate 1000 rows, run anonymization, if entities were found,
        add to dict col name and set of entity types,
        e.g. result.update({'col': column_name, 'ents': [PER, CONTACTS, DATE], 'type': 'text'})
        - if it is of type date
        e.g. result.update({'col': column_name, 'ents': [DATE], 'type': 'date'})
        - if it is of type integer
        e.g. result.update({'col': column_name, 'ents': [SENSITIVE], 'type': 'int'})
        - if it is of type float
        e.g. result.update({'col': column_name, 'ents': [SENSITIVE], 'type': 'float'})
        """

        def _analyze_if_strategy_is_text(entities, chunk):
            """if mean lenght of content is less than 30, and there are one or two entities,
            then we do not need complex ml analysis for text as the column contains one entity"""
            if len(entities) < 3 and chunk.apply(lambda x: len(str(x))).mean() < 30:
                return False
            return True

        def _get_top_values(chunk, col, top=4):
            top_values = chunk[col].value_counts().index.tolist()
            top_values = [str(val) for val in top_values]
            return top_values[:top]

        result = {}
        EXCLUDED_COLUMNS = ["id", "created_at", "updated_at"]

        # get column names and data types
        column_types = await self._retrive_column_types(params.src_table_name)
        column_names = [col["column_name"] for col in column_types]

        # get references
        references = await self._retrieve_references(params.src_table_name)
        logger.info(f"\033[096mReferences: {references}\033[0m")

        try:
            idx = 0
            async for chunk in data_stream:
                logger.info(f"Processing chunk {idx}...")
                update_job_status(job_id, params=params, message=f"CURRENT CHUNK: {idx}")
                chunk = pd.DataFrame(chunk)
                for col in chunk.columns:
                    column_name = column_names[col]
                    is_reference = column_name in references
                    top_values = _get_top_values(chunk, col)

                    if col not in result:
                        result[column_name] = {'ents': list(),
                                               'type': 'unknown',
                                               'reference': is_reference,
                                               'top_values': top_values}

                    if column_name in EXCLUDED_COLUMNS:
                        continue

                    if chunk[col].dtype == 'object':
                        # concatenate 1000 rows
                        text = ' '.join(chunk[col][:1000].astype(str))
                        if text:
                            # set up two configs
                            config1 = ConfigNER()
                            config1.aggressive = False
                            config2 = ConfigNER()
                            config2.aggressive = True

                            # run in standard mode
                            predictions = await anonymize_concatenated_text(text, request, config1)

                            # get entities
                            entities = [ent['label'] for ent in predictions['personal_data']]

                            # if entities were found, add to dict col name and set of entity types
                            if entities:
                                entities = list(set(entities))
                                is_text = _analyze_if_strategy_is_text(entities, chunk[col])
                                if is_text:
                                    entities = entities + ['TEXT', ]
                                result[column_name] = {'ents': entities, 'type': 'text'}

                            # otherwise, just add the empty list and 'text' type
                            else:
                                # rerun in aggressive mode
                                predictions = await anonymize_concatenated_text(text, request, config2)
                                entities = [ent['label'] for ent in predictions['personal_data']]
                                if entities:
                                    result[column_name] = {'ents': ['SENSITIVE'], 'type': 'text'}
                                else:
                                    result[column_name] = {'ents': list(), 'type': 'text'}

                    elif chunk[col].dtype == 'datetime64':
                        result[column_name] = {'ents': ['DATE'], 'type': 'date'}

                    elif chunk[col].dtype == 'int64':
                        result[column_name] = {'ents': ['SENSITIVE'], 'type': 'int'}

                    elif chunk[col].dtype == 'float64':
                        result[column_name] = {'ents': ['SENSITIVE'], 'type': 'float'}

                    elif chunk[col].dtype == 'bool':
                        result[column_name] = {'ents': [], 'type': 'bool'}

                    result[column_name]['top_values'] = top_values
                    result[column_name]['unique_values'] = chunk[col].nunique()
                    result[column_name]['reference'] = is_reference

                idx += 1
            update_job_status(job_id, params=params, message="FINISHED")
            logger.info(f"\033[092mColumns analyzed: {result.keys()}\033[0m")  # ###########################
            return result

        except Exception as e:
            logger.error(f"Failed to profile data: {e}")
            update_job_status(job_id, params=params, message=f"FAILED: {e}")
            raise HTTPException(status_code=500, detail=str(e))

    async def _retrive_column_types(self, table_name):
        column_types = await self.connection.fetch(f"""
            SELECT column_name, data_type
            FROM information_schema.columns
            WHERE table_schema = 'public' AND table_name = '{table_name}'
        """)
        column_types = [{"column_name": col["column_name"], "data_type": col["data_type"]} for col in column_types]
        return column_types

    async def _retrieve_references(self, table_name):
        references = await self.connection.fetch(f"""
            SELECT
                tc.table_name, kcu.column_name, ccu.table_name AS foreign_table_name, ccu.column_name AS foreign_column_name
            FROM
                information_schema.table_constraints AS tc
                JOIN information_schema.key_column_usage AS kcu
                    ON tc.constraint_name = kcu.constraint_name
                JOIN information_schema.constraint_column_usage AS ccu
                    ON ccu.constraint_name = tc.constraint_name
            WHERE constraint_type = 'FOREIGN KEY' AND tc.table_name = '{table_name}'
        """)
        cols_with_refs = [ref["column_name"] for ref in references]
        return cols_with_refs

    async def _get_row_count(self, table_name):
        row_count = await self.connection.fetchval(f"SELECT COUNT(*) FROM {table_name}")
        return row_count

    async def stream_data(self, table_name: str, chunk_size: int = 100, limit: int = None) -> \
            AsyncGenerator[list, None]:
        offset = 0
        total_rows = await self._get_row_count(table_name)

        if limit:
            total_rows = min(total_rows, limit)

        while offset < total_rows:
            query = f"""
                SELECT * FROM {table_name}
                OFFSET {offset} LIMIT {chunk_size}
            """
            rows = await self.connection.fetch(query)
            if not rows:
                break
            yield rows
            offset += chunk_size

    async def get_entire_table_as_dataframe(self, table_name: str, limit=None) -> pd.DataFrame:
        """Retrieve the entire table as a DataFrame. Optionally, limit the number of rows."""
        query = f"SELECT * FROM {table_name}" + (f" LIMIT {limit}" if limit else "")
        rows = await self.connection.fetch(query)
        if not rows:
            return pd.DataFrame()

        columns = rows[0].keys()
        data = [dict(row) for row in rows]
        return pd.DataFrame(data, columns=columns)


def update_job_status(job_id, params, message):
    end = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    job_data = pd.read_csv(params.logfile)
    job_data.loc[job_data["job_id"] == job_id, "status"] = message
    job_data.loc[job_data["job_id"] == job_id, "end"] = end
    job_data.to_csv(params.logfile, index=False)
