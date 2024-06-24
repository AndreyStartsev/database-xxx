import requests
import os
import json
import streamlit as st
import streamlit_shadcn_ui as ui
import pandas as pd
from time import time, sleep
from requests.exceptions import JSONDecodeError
from loguru import logger


API_PORT = os.getenv("API_PORT", 80)
API_HOST = os.getenv("API_HOST", "localhost")
API_KEY = os.getenv('API_APP_KEY', 'admin')
API_DB_KEY = os.getenv('API_DB_KEY', 'admin')

ENDPOINT_RETRIEVE_TABLE_DATA = '/api/postgres/show_head'
ENDPOINT_RETRIEVE_TABLE_NAMES = '/api/postgres/show_public_tables'


def safe_eval(string):
    if isinstance(string, dict):
        return string
    try:
        return json.loads(string.replace("'", '"'))
    except json.JSONDecodeError as e:
        print(f"Error parsing string: {string} - {e}")
        return {}


def _get_table_data(table_name, limit=10):
    """
    Profile database tables
    """
    url = f"http://{API_HOST}:{API_PORT}{ENDPOINT_RETRIEVE_TABLE_DATA}"
    params = {
        "table_name": table_name,
        "limit": limit
    }

    headers = {"accept": "application/json", "xxx": API_DB_KEY}
    start = time()

    try:
        logger.info(f"API request: {url}")
        res = requests.get(url=url, headers=headers, params=params)
        logger.info(f"API response: {res.text}, time: {time() - start:.2f} sec")
        return res.json()
    except JSONDecodeError:
        logger.error(f"JSONDecodeError: {res.text}")
        return {"error": res.text}
    except Exception as e:
        logger.error(f"Error: {e}")
        return {"error": f"{e}"}


def _get_table_names():
    """
    Get table names from the database
    """
    url = f"http://{API_HOST}:{API_PORT}{ENDPOINT_RETRIEVE_TABLE_NAMES}"
    headers = {"accept": "application/json", "xxx": API_DB_KEY}
    start = time()

    try:
        logger.info(f"API request: {url}")
        res = requests.get(url=url, headers=headers)
        logger.info(f"API response: {res.text}, time: {time() - start:.2f} sec")
        return res.json()
    except JSONDecodeError:
        logger.error(f"JSONDecodeError: {res.text}")
        return {"error": res.text}
    except Exception as e:
        logger.error(f"Error: {e}")
        return {"error": f"{e}"}


def get_table_names():
    """
    Get table names from the database
    """
    json_response = _get_table_names()
    if json_response.get("error"):
        return []
    table_names = [tdict["tablename"] for tdict in json_response["tables"]]
    tables_main = [tname for tname in table_names if not tname.startswith("anonymized")]
    tables_anon = [tname for tname in table_names if tname.startswith("anonymized")]
    return tables_main, tables_anon


def fetch_random_sample(table_name: str, limit: int) -> pd.DataFrame:
    """
    Fetch a random sample from a table
    """
    json_response = _get_table_data(table_name, limit)
    if json_response.get("error"):
        print(json_response)
        return pd.DataFrame()
    elif not json_response.get("table"):
        print(json_response)
        st.write(json_response)
        return pd.DataFrame()
    table_data = pd.DataFrame(json_response["table"])
    return table_data


def main():
    table_names, tables_anon = get_table_names()
    with st.expander("База данных содержит следующие анонимизированные таблицы"):
        st.write(tables_anon)
    col1, col2 = st.columns((6, 4))

    with col1:
        table_name = st.selectbox("Enter table name", table_names, index=0)
        table_anon = f"anonymized_{table_name}"
        is_anon = table_anon in tables_anon
        n_entries = st.number_input("Enter number of entries", min_value=1, value=10, step=1, max_value=100)
        stats_placeholder = st.empty()
    with col2:
        df_placeholder = st.empty()

    if st.button("Get data"):
        df = fetch_random_sample(table_name=table_name, limit=n_entries)
        anon_df = pd.DataFrame()
        if is_anon:
            anon_df = fetch_random_sample(table_name=table_anon, limit=n_entries)
        st.session_state["df"] = df
        st.session_state["anon_df"] = anon_df

    if st.session_state.get("df", None) is not None:
        df = st.session_state["df"]
        anon_df = st.session_state["anon_df"]
        df_placeholder.dataframe(df, use_container_width=True, height=300)
        anon_icon = "✅" if is_anon else "⚪"
        st.info(f"ТАБЛИЦА: {table_name} | ЗАПИСЕЙ: {len(df)} | АНОНИМИЗИРОВАНО: {is_anon} {anon_icon}")
        st.write(f"columns: {df.columns.tolist()}")

        col1, col2 = st.columns((6, 4))
        with col1:
            if "config" in st.session_state.keys() and is_anon:
                st.write(st.session_state["config"][table_name])
        with col2:
            sorted_cols = [col for col in df.columns if col in anon_df.columns]
            anon_df = anon_df[sorted_cols]
            # st.dataframe(anon_df, height=300, use_container_width=True)
            st.write(anon_df, width=300)


if __name__ == "__main__":
    names = get_table_names()
    print(names)

    table_name = names[0]
    n_entries = 4
    data_json = fetch_random_sample(table_name=table_name, limit=n_entries)
    print(data_json)
