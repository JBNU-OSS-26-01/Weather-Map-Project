# Weather Map Project

`Streamlit + FastAPI + SQLite3` 기반의 공공데이터포털 중기예보 지도 앱 프로젝트입니다.

## Stack

- Frontend: Streamlit
- Backend: FastAPI
- Database: SQLite3
- Environment / Package Manager: uv

## Quick Start

```bash
uv sync
uv run uvicorn backend.main:app --reload
uv run streamlit run frontend/app.py
```

## Main Features

- CSV to SQLite initial import
- JWT register/login
- Forecast CRUD API
- Streamlit map dashboard with color-based forecast overlay
- Docker compose for frontend and backend

## Docker

```bash
docker compose up --build
```

- Frontend: http://localhost:8501
- Backend: http://localhost:8000
- Swagger: http://localhost:8000/docs

## Structure

```text
.
|-- backend/
|   |-- api/
|   |   `-- routes/
|   |       `-- weather.py
|   |-- core/
|   |   `-- config.py
|   |-- db/
|   |   `-- database.py
|   `-- main.py
|-- frontend/
|   |-- components/
|   `-- app.py
|-- data/
|   `-- weather.db
`-- pyproject.toml
```
