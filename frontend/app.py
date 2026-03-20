import os

import streamlit as st


st.set_page_config(page_title="Weather Map", page_icon="🌤️", layout="wide")

backend_base_url = os.getenv("BACKEND_BASE_URL", "http://127.0.0.1:8000")

st.title("중기예보 지도 앱")
st.caption("Streamlit 프론트엔드 초기 화면입니다.")

st.info("프론트엔드 구조를 먼저 초기화했습니다. 기능 구현은 다음 단계에서 진행하면 됩니다.")
st.write(f"Backend URL: `{backend_base_url}`")
