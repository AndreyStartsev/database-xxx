from PIL import Image
import streamlit as st

from utils.auth import login, reset_login
from cfg.constants import LOGO


def main():
    col1, col2 = st.columns((2, 3))

    with col1:
        if "username" not in st.session_state.keys():
            login()
        else:
            username = st.session_state["username"]
            st.markdown(f"You are successfully logged, {username}! ✅")
            if st.button("Log out"):
                reset_login()
    with col2:
        st.image(LOGO, width=100, use_column_width=False)


if __name__ == "__main__":
    main()