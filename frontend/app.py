import os

import pandas as pd
import pydeck as pdk
import requests
import streamlit as st


st.set_page_config(page_title="Weather Map", page_icon="🌤️", layout="wide")

backend_base_url = os.getenv("BACKEND_BASE_URL", "http://127.0.0.1:8000")
forecast_colors = {
    "맑음": "#2ecc71",
    "구름많음": "#f1c40f",
    "흐림": "#7f8c8d",
    "흐리고 비": "#3498db",
}


def init_state() -> None:
    st.session_state.setdefault("token", None)
    st.session_state.setdefault("user", None)
    st.session_state.setdefault("map_loaded", False)


def inject_styles() -> None:
    st.markdown(
        """
        <style>
        :root {
            --ink-strong: #10233f;
            --ink-base: #25415f;
            --ink-soft: #5e7691;
            --glass-bg: rgba(255, 255, 255, 0.42);
            --glass-stroke: rgba(255, 255, 255, 0.62);
            --glass-shadow: 0 24px 60px rgba(18, 38, 63, 0.12);
            --accent-cyan: #1ca3c7;
            --accent-mint: #22c78a;
            --accent-sun: #f4bd34;
            --accent-rain: #3388d6;
        }
        .stApp {
            background:
                radial-gradient(circle at 10% 15%, rgba(34, 199, 138, 0.22), transparent 22%),
                radial-gradient(circle at 88% 12%, rgba(28, 163, 199, 0.22), transparent 24%),
                radial-gradient(circle at 50% 85%, rgba(244, 189, 52, 0.12), transparent 20%),
                linear-gradient(145deg, #e6f4ff 0%, #eefaf7 34%, #f8fbff 62%, #fcf8f0 100%);
            color: var(--ink-strong);
        }
        .stApp::before {
            content: "";
            position: fixed;
            inset: 0;
            pointer-events: none;
            background-image:
                linear-gradient(rgba(255,255,255,0.14) 1px, transparent 1px),
                linear-gradient(90deg, rgba(255,255,255,0.14) 1px, transparent 1px);
            background-size: 32px 32px;
            mask-image: radial-gradient(circle at center, black 45%, transparent 90%);
            opacity: 0.45;
        }
        .block-container {
            padding-top: 2rem;
            padding-bottom: 2rem;
        }
        h1, h2, h3, h4, p, label, div, span {
            color: var(--ink-strong);
        }
        .stCaptionContainer, .stMarkdown p, .stText, .st-emotion-cache-10trblm {
            color: var(--ink-base);
        }
        section[data-testid="stSidebar"] {
            background: linear-gradient(180deg, rgba(14, 30, 51, 0.82), rgba(25, 52, 84, 0.74));
            border-right: 1px solid rgba(255, 255, 255, 0.18);
            backdrop-filter: blur(22px);
        }
        section[data-testid="stSidebar"] * {
            color: #eff8ff !important;
        }
        section[data-testid="stSidebar"] .stCodeBlock code {
            color: #d8f3ff !important;
        }
        .hero-shell {
            position: relative;
            overflow: hidden;
            background:
                linear-gradient(135deg, rgba(255,255,255,0.55), rgba(255,255,255,0.24)),
                linear-gradient(120deg, rgba(28,163,199,0.12), rgba(34,199,138,0.1));
            border: 1px solid var(--glass-stroke);
            box-shadow: var(--glass-shadow);
            backdrop-filter: blur(18px);
            border-radius: 28px;
            padding: 1.6rem 1.8rem;
            margin-bottom: 1rem;
        }
        .hero-shell::after {
            content: "";
            position: absolute;
            width: 240px;
            height: 240px;
            right: -80px;
            top: -90px;
            border-radius: 999px;
            background: radial-gradient(circle, rgba(28,163,199,0.26), transparent 70%);
        }
        .hero-kicker {
            display: inline-block;
            font-size: 0.82rem;
            letter-spacing: 0.08em;
            text-transform: uppercase;
            color: var(--accent-cyan);
            background: rgba(255,255,255,0.62);
            border: 1px solid rgba(28,163,199,0.14);
            padding: 0.35rem 0.7rem;
            border-radius: 999px;
            margin-bottom: 0.8rem;
        }
        .hero-title {
            font-size: 2.2rem;
            line-height: 1.08;
            margin: 0 0 0.55rem 0;
            font-weight: 800;
            color: var(--ink-strong);
        }
        .hero-copy {
            margin: 0;
            font-size: 1rem;
            line-height: 1.7;
            color: var(--ink-base);
        }
        .glass-card {
            background: var(--glass-bg);
            border: 1px solid var(--glass-stroke);
            box-shadow: var(--glass-shadow);
            backdrop-filter: blur(18px);
            border-radius: 24px;
            padding: 1.25rem 1.4rem;
            margin-bottom: 1rem;
        }
        .stats-grid {
            display: grid;
            grid-template-columns: repeat(2, minmax(0, 1fr));
            gap: 0.85rem;
        }
        .stat-card {
            background: rgba(255, 255, 255, 0.48);
            border: 1px solid rgba(255, 255, 255, 0.68);
            border-radius: 20px;
            padding: 0.95rem 1rem;
            min-height: 108px;
            box-shadow: inset 0 1px 0 rgba(255,255,255,0.35);
        }
        .stat-label {
            font-size: 0.82rem;
            color: var(--ink-soft);
            margin-bottom: 0.35rem;
        }
        .stat-value {
            font-size: 1.3rem;
            font-weight: 800;
            color: var(--ink-strong);
            margin-bottom: 0.2rem;
        }
        .stat-meta {
            font-size: 0.85rem;
            color: var(--ink-base);
        }
        .panel-title {
            margin: 0 0 0.4rem 0;
            color: var(--ink-strong);
            font-size: 1.1rem;
            font-weight: 700;
        }
        .panel-copy {
            margin: 0;
            color: var(--ink-base);
            line-height: 1.65;
        }
        .legend-wrap {
            display: flex;
            flex-wrap: wrap;
            gap: 0.55rem;
        }
        .legend-chip {
            display: inline-block;
            padding: 0.45rem 0.8rem;
            border-radius: 999px;
            font-size: 0.88rem;
            font-weight: 700;
            color: var(--ink-strong);
            background: rgba(255,255,255,0.78);
            border: 1px solid rgba(255,255,255,0.82);
            box-shadow: 0 10px 24px rgba(15, 23, 42, 0.05);
        }
        .toolbar-card {
            background: linear-gradient(180deg, rgba(255,255,255,0.56), rgba(255,255,255,0.38));
        }
        .stButton > button {
            border-radius: 16px;
            border: 1px solid rgba(17, 24, 39, 0.04);
            background: linear-gradient(135deg, #0c8fb8 0%, #1ec98a 100%);
            color: white !important;
            font-weight: 700;
            box-shadow: 0 18px 36px rgba(14, 113, 130, 0.2);
        }
        .stSelectbox label, .stSegmentedControl label {
            color: var(--ink-base) !important;
            font-weight: 600;
        }
        [data-testid="stSegmentedControl"] button {
            color: var(--ink-strong) !important;
        }
        div[data-testid="stDataFrame"] {
            background: rgba(255,255,255,0.5);
            border: 1px solid rgba(255,255,255,0.7);
            border-radius: 24px;
            padding: 0.4rem;
            box-shadow: var(--glass-shadow);
        }
        @media (max-width: 768px) {
            .hero-title {
                font-size: 1.7rem;
            }
            .stats-grid {
                grid-template-columns: 1fr;
            }
        }
        </style>
        """,
        unsafe_allow_html=True,
    )


def api_request(method: str, path: str, *, token: str | None = None, **kwargs):
    headers = kwargs.pop("headers", {})
    if token:
        headers["Authorization"] = f"Bearer {token}"
    return requests.request(
        method,
        f"{backend_base_url}{path}",
        headers=headers,
        timeout=20,
        **kwargs,
    )


def render_auth_panel() -> None:
    with st.sidebar:
        st.markdown("### 연결 정보")
        st.code(backend_base_url)

        if st.session_state.user:
            st.success(f"{st.session_state.user['username']} 님 로그인됨")
            if st.button("로그아웃", use_container_width=True):
                st.session_state.token = None
                st.session_state.user = None
                st.rerun()
            return

        login_tab, register_tab = st.tabs(["로그인", "회원가입"])

        with login_tab:
            with st.form("login_form"):
                username = st.text_input("아이디", key="login_username")
                password = st.text_input("비밀번호", type="password", key="login_password")
                submitted = st.form_submit_button("로그인", use_container_width=True)
                if submitted:
                    response = api_request(
                        "POST",
                        "/auth/login",
                        json={"username": username, "password": password},
                    )
                    if response.ok:
                        payload = response.json()
                        st.session_state.token = payload["access_token"]
                        st.session_state.user = payload["user"]
                        st.success("로그인에 성공했습니다.")
                        st.rerun()
                    else:
                        st.error(response.json().get("detail", "로그인에 실패했습니다."))

        with register_tab:
            with st.form("register_form"):
                username = st.text_input("새 아이디", key="register_username")
                email = st.text_input("이메일", key="register_email")
                password = st.text_input("새 비밀번호", type="password", key="register_password")
                submitted = st.form_submit_button("회원가입", use_container_width=True)
                if submitted:
                    response = api_request(
                        "POST",
                        "/auth/register",
                        json={"username": username, "email": email, "password": password},
                    )
                    if response.ok:
                        st.success("회원가입이 완료되었습니다. 이제 로그인해 주세요.")
                    else:
                        st.error(response.json().get("detail", "회원가입에 실패했습니다."))


def fetch_forecast_options() -> dict:
    try:
        response = api_request("GET", "/forecasts/options")
        response.raise_for_status()
        return response.json()
    except requests.RequestException:
        return {
            "latest_batch_id": None,
            "available_dates": [],
            "available_time_periods": [],
        }


def fetch_map_data(forecast_date: str, time_period: str) -> list[dict]:
    try:
        response = api_request(
            "GET",
            "/forecasts/map",
            params={"forecast_date": forecast_date, "time_period": time_period},
        )
        response.raise_for_status()
        return response.json()
    except requests.RequestException:
        return []


def render_map(map_rows: list[dict], active: bool) -> None:
    if active and map_rows:
        dataframe = pd.DataFrame(map_rows)
    else:
        fallback_regions = api_request("GET", "/regions").json()
        dataframe = pd.DataFrame(fallback_regions)
        dataframe["region_name"] = dataframe["name"]
        dataframe["color_rgba"] = [[180, 190, 200, 120]] * len(dataframe)
        dataframe["forecast_label"] = "대기중"
        dataframe["precipitation_probability"] = 0

    layer = pdk.Layer(
        "ScatterplotLayer",
        data=dataframe,
        get_position="[longitude, latitude]",
        get_fill_color="color_rgba",
        get_radius=45000,
        pickable=True,
        opacity=0.85,
        stroked=True,
        get_line_color=[255, 255, 255, 180],
        line_width_min_pixels=2,
    )

    tooltip = {
        "html": """
        <b>{region_name}</b><br/>
        예보: {forecast_label}<br/>
        강수확률: {precipitation_probability}%
        """,
        "style": {"backgroundColor": "#0f172a", "color": "white"},
    }

    deck = pdk.Deck(
        map_style="light",
        initial_view_state=pdk.ViewState(
            latitude=36.35,
            longitude=127.8,
            zoom=6.4,
            pitch=0,
        ),
        layers=[layer],
        tooltip=tooltip,
    )
    st.pydeck_chart(deck, use_container_width=True)


def render_main() -> None:
    options = fetch_forecast_options()
    available_dates = options["available_dates"]
    available_periods = options["available_time_periods"]

    if not available_dates or not available_periods:
        st.warning("백엔드 연결 또는 예보 데이터 상태를 확인해 주세요.")
        render_map([], False)
        return

    selected_date_preview = available_dates[0]
    selected_period_preview = available_periods[0]

    st.markdown(
        f"""
        <div class="hero-shell">
            <div class="hero-kicker">Mid-term Forecast Intelligence</div>
            <h1 class="hero-title">중기예보를 지도로 읽는<br/>깔끔한 Forecast Studio</h1>
            <p class="hero-copy">
                SQLite에 적재된 예보 데이터를 FastAPI와 Streamlit으로 연결했습니다.
                버튼 한 번으로 전국 권역의 예보 상태를 색으로 확인하고, 강수확률 흐름까지 빠르게 파악할 수 있습니다.
            </p>
        </div>
        """,
        unsafe_allow_html=True,
    )

    top_left, top_right = st.columns([2.3, 1.2])
    with top_left:
        st.markdown(
            """
            <div class="glass-card">
                <div class="panel-title">전국 예보 맵</div>
                <p class="panel-copy">메인 지도는 한국 주요 권역 중심 좌표를 기준으로 구성되어 있습니다. 버튼을 누르면 지역별 중기예보가 즉시 색상으로 반영됩니다.</p>
            </div>
            """,
            unsafe_allow_html=True,
        )

    with top_right:
        st.markdown(
            f"""
            <div class="glass-card">
                <div class="stats-grid">
                    <div class="stat-card">
                        <div class="stat-label">Backend</div>
                        <div class="stat-value">API Ready</div>
                        <div class="stat-meta">{backend_base_url}</div>
                    </div>
                    <div class="stat-card">
                        <div class="stat-label">Latest Batch</div>
                        <div class="stat-value">{options['latest_batch_id']}</div>
                        <div class="stat-meta">가장 최근 적재 예보</div>
                    </div>
                    <div class="stat-card">
                        <div class="stat-label">Date Range</div>
                        <div class="stat-value">{len(available_dates)}일</div>
                        <div class="stat-meta">{available_dates[0]} 시작</div>
                    </div>
                    <div class="stat-card">
                        <div class="stat-label">Default View</div>
                        <div class="stat-value">{selected_date_preview}</div>
                        <div class="stat-meta">{selected_period_preview} 기준</div>
                    </div>
                </div>
            </div>
            """,
            unsafe_allow_html=True,
        )

    selector_col, button_col = st.columns([1.2, 1.2])
    with selector_col:
        st.markdown('<div class="glass-card toolbar-card">', unsafe_allow_html=True)
        selected_date = st.selectbox("예보 날짜", available_dates, index=0)
        selected_period = st.segmented_control(
            "시간대",
            options=available_periods,
            default=available_periods[0] if available_periods else None,
        )
        st.markdown("</div>", unsafe_allow_html=True)
    with button_col:
        st.markdown(
            """
            <div class="glass-card toolbar-card">
                <div class="panel-title">레이어 컨트롤</div>
                <p class="panel-copy">예보 오버레이를 켜면 권역별 상태가 색상으로 나타납니다.</p>
            </div>
            """,
            unsafe_allow_html=True,
        )
        if st.button("지도에 예보 색상 표시", use_container_width=True, type="primary"):
            st.session_state.map_loaded = True

    map_rows: list[dict] = []
    if st.session_state.map_loaded and selected_date and selected_period:
        map_rows = fetch_map_data(selected_date, selected_period)

    st.markdown('<div class="glass-card">', unsafe_allow_html=True)
    render_map(map_rows, st.session_state.map_loaded)
    st.markdown("</div>", unsafe_allow_html=True)

    st.markdown(
        """
        <div class="glass-card">
            <div class="panel-title">Forecast Palette</div>
            <div class="legend-wrap">
                <div class="legend-chip" style="background:#2ecc7133;">맑음</div>
                <div class="legend-chip" style="background:#f1c40f33;">구름많음</div>
                <div class="legend-chip" style="background:#7f8c8d33;">흐림</div>
                <div class="legend-chip" style="background:#3498db33;">흐리고 비</div>
            </div>
        </div>
        """,
        unsafe_allow_html=True,
    )

    if map_rows:
        st.markdown(
            """
            <div class="glass-card">
                <div class="panel-title">Forecast Table</div>
                <p class="panel-copy">현재 선택한 날짜와 시간대의 지역별 예보를 표로 확인할 수 있습니다.</p>
            </div>
            """,
            unsafe_allow_html=True,
        )
        dataframe = pd.DataFrame(map_rows)[
            ["region_name", "forecast_date", "time_period", "forecast_label", "precipitation_probability"]
        ]
        dataframe.columns = ["지역", "날짜", "시간대", "예보", "강수확률(%)"]
        st.dataframe(dataframe, use_container_width=True, hide_index=True)


init_state()
inject_styles()
render_auth_panel()
render_main()
