from st_on_hover_tabs import on_hover_tabs
import streamlit as st

from app_pages.RESULTS import main as results_page
from app_pages.DATABASE import main as db_page
from utils.login import main as login_user
from cfg.constants import PAGE_CONFIG, STYLES

st.set_page_config(**PAGE_CONFIG)
st.markdown('<style>' + open('./style.css').read() + '</style>', unsafe_allow_html=True)

with st.sidebar:
    tabs = on_hover_tabs(tabName=['Login', 'Database', 'Results'],
                         iconName=['login', 'shield', 'code'],
                         default_choice=0,
                         styles=STYLES,
                         key="1")

if tabs == 'Login':
    st.title("Авторизация")
    login_user()

elif tabs == 'Database':
    st.title("Database")
    db_page(admin=True)

elif tabs == 'Results':
    st.title("Результаты")
    results_page()
