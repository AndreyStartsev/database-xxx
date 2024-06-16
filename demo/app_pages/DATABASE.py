import requests
import os
import json
import streamlit as st
import streamlit_shadcn_ui as ui
import pandas as pd
from time import time, sleep
from requests.exceptions import JSONDecodeError
from loguru import logger

from utils.auth import auth_simple
from data.demo_docs import DEMO_DOCS

API_PORT = os.getenv("API_PORT", 80)
API_HOST = os.getenv("API_HOST", "localhost")
API_KEY = os.getenv('API_APP_KEY', 'admin')
API_DB_KEY = os.getenv('API_DB_KEY', 'admin')

ENDPOINT_ANALYSIS = '/api/postgres/start-analysis'
ENDPOINT_ANONYMIZE = '/api/postgres/start-anonymization'

NER_DICT = {
    "PER": "Имена, фамилии, отчества",
    "LOC": "Локации",
    "ORG": "Организации",
    "DATE": "Даты",
    "CONTACTS": "Контакты",
    "SENSITIVE": "Прочие чувствительные данные",
    "PHONE": "Телефоны",
    "EMAIL": "Email",
    "URL": "URL",
}


def safe_eval(string):
    if isinstance(string, dict):
        return string
    try:
        return json.loads(string.replace("'", '"'))
    except json.JSONDecodeError as e:
        print(f"Error parsing string: {string} - {e}")
        return {}


def profile_database():
    """
    Profile database tables
    """
    url = f"http://{API_HOST}:{API_PORT}{ENDPOINT_ANALYSIS}"
    params = {
        "src_table_name": "employee",
        "analysis_type": "column",
        "logfile": "/code/logs/analysis_jobs.log",
        "dest_table_prefix": "",
        "result_folder": "/code/logs/",
    }

    headers = {"accept": "application/json", "xxx": API_DB_KEY}
    start = time()

    try:
        logger.info(f"API request: {url}")
        res = requests.post(url=url, headers=headers, json=params)
        logger.info(f"API response: {res.text}, time: {time() - start:.2f} sec")
        return res.json()
    except JSONDecodeError:
        logger.error(f"JSONDecodeError: {res.text}")
        return {"error": res.text}
    except Exception as e:
        logger.error(f"Error: {e}")
        return {"error": f"{e}"}


def anonymize_database(config: dict):
    """
curl -X 'POST' \
  'http://localhost/api/postgres/start-anonymization' \
  -H 'accept: application/json' \
  -H 'xxx: admin' \
  -H 'Content-Type: application/json' \
  -d '{
  "entries_limit": 0,
  "src_table_name": "employee",
  "dest_table_prefix": "anonymized",
  "dest_type": "csv",
  "dest_csv_file_folder": "/code/logs/",
  "logfile": "/code/logs/anonymization_jobs.log",
  "drop_existing_table": true
}'

    """
    url = f"http://{API_HOST}:{API_PORT}{ENDPOINT_ANONYMIZE}"
    params = {
        "entries_limit": 0,
        "src_table_name": "employee",
        "dest_table_prefix": "anonymized",
        "dest_type": "db",
        "columns": None,  # list of columns to analyze (optional), if None - all columns
        "strategy_by_column": None,  # dict with column names and strategies, if None - default strategy
        "drop_existing_table": True,
    }

    headers = {"accept": "application/json", "xxx": API_DB_KEY}
    start = time()
    job_ids = {}

    for table_name, table_config in config.items():
        params["src_table_name"] = table_name
        params["columns"] = table_config["columns"]
        params["strategy_by_column"] = table_config["strategy"]
        params["dest_table_prefix"] = table_config["prefix"]
        try:
            logger.info(f"API request: {url}")
            res = requests.post(url=url, headers=headers, json=params)
            logger.info(f"API response: {res.text}, time: {time() - start:.2f} sec")
            job_ids[table_name] = res.json()
        except JSONDecodeError:
            logger.error(f"JSONDecodeError: {res.text}")
            job_ids[table_name] = {"error": res.text}
        except Exception as e:
            logger.error(f"Error: {e}")
            job_ids[table_name] = {"error": f"{e}"}

    return job_ids


@auth_simple
def main(admin=False):
    # component: RANDOM TEXT
    with st.sidebar:
        st_reset_placeholder = st.empty()
    col1t, col2t = st.columns((6, 4))
    with col2t:
        st_button_placeholder = st.empty()
        debug = False # st.checkbox("Debug", key="debug", value=False, help="Debug mode")
        st_result_placeholder = st.empty()
    with col1t:
        st.markdown("[i] Перед анонимизацией необходимо проанализировать данные")
        show_all = st.checkbox("Показать все данные", key="show_all", value=False, help="Показать все таблицы")

    if st.session_state.get("data_json", None) is None or debug:
        update = st_button_placeholder.button("Profiling", type="primary", disabled=False)
        anonymize = False
    else:
        update = False
        anonymize = st_button_placeholder.button("Anonymize", type="primary", disabled=False)

    # logic: RANDOM TEXT
    if update:
        st_result_placeholder.info("Загрузка...")
        data_json = profile_database()
        st.write(data_json)
        if data_json.get("error"):
            st_result_placeholder.error(data_json["error"])
        elif not isinstance(data_json, dict):
            st_result_placeholder.error(f"Ошибка при загрузке данных: {data_json}")
        else:
            st_result_placeholder.success("Готово!")
            st.session_state["data_json"] = data_json

    with col1t:
        if st.session_state.get("data_json", None) is not None:
            data_json = st.session_state["data_json"]

            if st_reset_placeholder.button("Reset", type="secondary"):
                tables = list(data_json.keys())
                for table_name in tables:
                    st.session_state.pop(table_name, None)
                st.session_state.pop("data_json", None)

            for table_name, profiling_data in data_json.items():
                if table_name.startswith("anonymized"):
                    continue

                st.markdown(f"### {table_name}")

                if st.session_state.get(table_name, None) is None:
                    st.session_state[table_name] = {'table_data': None, 'include': False, 'columns': {}}

                col1, col2 = st.columns((2, 6))
                parsed_data = {key: safe_eval(value) for key, value in profiling_data.items()}

                # exclude if 'is_reference' is True
                sensitive_data = {key: value for key, value in parsed_data.items() if
                                  (not value.get("reference", False) and
                                   not key == 'id')}
                # exclude if 'ents' is empty
                sensitive_data = {key: value for key, value in sensitive_data.items() if value.get("ents", [])}

                st.session_state[table_name]['table_data'] = sensitive_data

                if not show_all:
                    parsed_data = sensitive_data

                with col2:
                    try:
                        df = pd.DataFrame(parsed_data).T
                        df.rename(columns={"ents": "anonymization strategy"}, inplace=True)
                        st.dataframe(df, width=800)
                    except Exception as e:
                        logger.error(f"Error: {e}")
                        st.error(f"Error: {e}")

                if len(sensitive_data.keys()) == 0:
                    with col1:
                        st.info("Чувствительные данные не обнаружены")
                    continue

                with col1:
                    defaul_checked = True if not table_name.startswith("anonymized") else False
                    tab_check = ui.switch(default_checked=defaul_checked, label=f"Table {table_name}",
                                          key=f"check_{table_name}")
                    if tab_check:
                        st.session_state[table_name]['include'] = True
                    else:
                        st.session_state[table_name]['include'] = False

                with col1:
                    st.markdown(f"##### Включить колонки:")
                    for idx, col in enumerate(sensitive_data.keys()):
                        disabled = not st.session_state[table_name]['include']
                        col_el_key = f"check_{table_name}_{col}"
                        options = st.session_state[table_name]['table_data'][col].get("ents", []) + ["SKIP", ]
                        options += [k for k in NER_DICT.keys() if k not in options]
                        col_strategy = st.selectbox(f"{col}", options=options, key=col_el_key,
                                                    disabled=disabled)

                        if col_strategy == "SKIP":
                            st.session_state[table_name]['columns'][col] = False
                        else:
                            st.session_state[table_name]['columns'][col] = col_strategy
                st.markdown("----")

    with col2t:
        with st.expander("Помощь", expanded=True):
            description = """
## Профайлинг таблиц базы данных

Каждая таблица анализируется на предмет наличия персональных данных. 

Алгоритм анализа:
- определяется тип для каждого столбца
- столбец, содержащий текст, анализируется на предмет наличия персональных данных с помощью МЛ-модели,
все найденные типы персональных данных помечаются в таблице
- столбцы, содержащие даты, помечаются соответствующим образом
- столбцы, содержащие числа, помечаются как чувствительные если не являются ключами

## Настройки анонимизации

Настройки анонимизации задаются в виде JSON-файла.

## Описание стратегий анонимизации

- SENSITIVE - генерация случайного числа
- LOC - генерация случайного города
- DATE - генерация случайной даты
- PHONE - генерация случайного телефона
- EMAIL - генерация случайного email
- URL - генерация случайного URL
- PER - генерация случайного имени
- ORG - генерация случайного названия организации
- TEXT - анализ текста с помощью МЛ-модели и замена всех найденных персональных данных на плейсхолдеры

## Процесс анонимизации
1. Выполните профайлинг таблиц
2. Выберите таблицы и столбцы для анонимизации, задайте стратегии анонимизации
3. Нажмите кнопку "Анонимизировать"
4. Дождитесь завершения процесса анонимизации
5. Проверьте результаты в разделе "Results" или непосредственно в базе данных

## Принцип работы генератора данных
- Генератор данных создает случайные значения в соответствии с выбранной стратегией
- Для каждого уникального значения генерируется одно и то же случайное значение
- Генерация выполняется для всей записи полностью кроме стратегии "TEXT"
- Для стратегии "TEXT" генерация выполняется отдельно для каждой найденной сущности, 
которая отмечена как персональные данные, такой подход позволяет сохранить контекст и структуру текста.

            """

            st.markdown(description, unsafe_allow_html=True)

        with st.expander("Настройки анонимизации БД"):
            use_strategy = st.checkbox("Использовать стратегии анонимизации", value=True)
            prefix = st.text_input("Префикс для новых таблиц", value="anonymized")

        if st.session_state.get("data_json", None) is not None:
            config = {}
            data_json = st.session_state["data_json"]
            for table_name, profiling_data in data_json.items():
                included = st.session_state.get(table_name, {}).get('include', False)

                if included:
                    sensitive_data = st.session_state[table_name]['table_data']
                    columns = list(sensitive_data.keys())
                    columns = [col for col in columns if st.session_state[table_name]['columns'].get(col, False)]
                    columns_strategy = {col: st.session_state[table_name]['columns'].get(col, "SENSITIVE")
                                        for col in columns} if use_strategy else None
                    config[table_name] = {
                        "columns": columns,
                        "strategy": columns_strategy,
                        "prefix": prefix if prefix else "anonymized"
                    }
                    # st.info(f"Table: {table_name}, columns: {columns}")

            st.session_state["config"] = config
            with st.expander("Конфигурация анонимизации"):
                st.write(config)

        if anonymize:
            st_result_placeholder.info("Анонимизация...")
            config = st.session_state.get("config", None)
            if config is None:
                st_result_placeholder.error("Не выбраны данные для анонимизации")
            else:
                with st.spinner("Анонимизация..."):
                    job_ids = anonymize_database(config)
                    st.write(job_ids)
                st_result_placeholder.success("Готово!")
