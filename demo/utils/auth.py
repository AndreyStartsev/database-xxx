import os

import streamlit as st
from loguru import logger

TEST_USER = os.getenv("TEST_USER", "test")
TEST_PASSWORD = os.getenv("TEST_PASSWORD", "test")
ADMINS = ["mary", "test"]
logger.info(f"\033[1;32;40mUSER: {TEST_USER} | {TEST_PASSWORD}\033[0m")


def mock_login(user, password):
    logger.info(f"\033[1;32;40m", f"User: {user}, password: {password}", "\033[0m")
    if user in ADMINS and password == "secret":
        st.session_state["token"] = None
        return True
    elif user == TEST_USER and password == TEST_PASSWORD:
        st.session_state["token"] = password
        return True
    return False


def reset_login():
    for key in st.session_state.keys():
        if key == "username":
            del st.session_state[key]
            del st.session_state["token"]


def login():
    placeholder = st.empty()
    with placeholder:
        with st.form("login"):
            user = st.text_input("Name", max_chars=10)
            password = st.text_input("Password", type="password", label_visibility="collapsed")
            submit = st.form_submit_button("Log in")
    if submit:
        valid = mock_login(user, password)
        if valid:
            st.session_state["username"] = user
            with placeholder:
                st.success(f"Welcome {_get_user()}!")
        else:
            st.warning("Login failed. Please try again.")


def _get_user():
    if 'username' in st.session_state.keys():
        user = st.session_state["username"].split("@")[0]
        return user


def auth_simple(func):
    OK = '\033[92m'  # GREEN
    INFO = '\033[90m'  # GRAY
    FAIL = '\033[91m'  # RED
    RESET = '\033[0m'  # RESET

    def wrapper(*args, **kwargs):
        admin = kwargs.get("admin", False)
        print(INFO, f"Authenticating {'ADMIN' if admin else 'USER'}...", end=" ")
        auth_user = _get_user()
        if auth_user in ADMINS:
            func()
            print(OK, "OK", RESET)
        elif auth_user and not admin:
            func()
            print(OK, "OK", RESET)
        elif auth_user is None:
            st.warning("You should log in first. Please go to the main page.")
        else:
            st.warning(f"Access denied for user {auth_user}")
            print(FAIL, "FAILED", RESET)

    return wrapper
