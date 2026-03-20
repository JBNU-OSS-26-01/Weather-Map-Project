import sqlite3
from datetime import datetime

import pandas as pd

from backend.core.config import CSV_PATH, DATA_DIR, DB_PATH

REGION_METADATA = {
    "서울.인천.경기": {"code": "SU", "latitude": 37.5665, "longitude": 126.9780},
    "강원영서": {"code": "GW_W", "latitude": 37.8813, "longitude": 127.7298},
    "강원영동": {"code": "GW_E", "latitude": 37.7519, "longitude": 128.8761},
    "충청북도": {"code": "CB", "latitude": 36.6357, "longitude": 127.4917},
    "충청남도": {"code": "CN", "latitude": 36.6588, "longitude": 126.6728},
    "전북자치도": {"code": "JB", "latitude": 35.8200, "longitude": 127.1088},
    "전라남도": {"code": "JN", "latitude": 34.8161, "longitude": 126.4630},
    "경상북도": {"code": "GB", "latitude": 36.5760, "longitude": 128.5056},
    "경상남도": {"code": "GN", "latitude": 35.2383, "longitude": 128.6924},
    "제주도": {"code": "JJ", "latitude": 33.4996, "longitude": 126.5312},
}

FORECAST_SCORES = {
    "맑음": 4,
    "구름많음": 3,
    "흐림": 2,
    "흐리고 비": 1,
}

TIME_PERIODS = {"오전": "AM", "오후": "PM"}


def get_connection() -> sqlite3.Connection:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    connection = sqlite3.connect(DB_PATH)
    connection.row_factory = sqlite3.Row
    connection.execute("PRAGMA foreign_keys = ON")
    return connection


def init_db() -> None:
    with get_connection() as connection:
        _reset_legacy_schema_if_needed(connection)
        connection.executescript(
            """
            CREATE TABLE IF NOT EXISTS users (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                username TEXT NOT NULL UNIQUE,
                email TEXT NOT NULL UNIQUE,
                password_hash TEXT NOT NULL,
                is_active INTEGER NOT NULL DEFAULT 1 CHECK (is_active IN (0, 1)),
                created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                CHECK (length(username) >= 3),
                CHECK (instr(email, '@') > 1)
            );

            CREATE TABLE IF NOT EXISTS regions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                code TEXT NOT NULL UNIQUE,
                name TEXT NOT NULL UNIQUE,
                latitude REAL NOT NULL CHECK (latitude BETWEEN 30 AND 40),
                longitude REAL NOT NULL CHECK (longitude BETWEEN 124 AND 132),
                created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
            );

            CREATE TABLE IF NOT EXISTS forecast_batches (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                raw_published_at TEXT NOT NULL UNIQUE,
                published_at TEXT NOT NULL UNIQUE,
                source_file TEXT NOT NULL,
                row_count INTEGER NOT NULL CHECK (row_count >= 0),
                imported_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
            );

            CREATE TABLE IF NOT EXISTS forecasts (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                batch_id INTEGER NOT NULL,
                region_id INTEGER NOT NULL,
                forecast_date TEXT NOT NULL,
                time_period TEXT NOT NULL CHECK (time_period IN ('AM', 'PM')),
                forecast_label TEXT NOT NULL CHECK (
                    forecast_label IN ('맑음', '구름많음', '흐림', '흐리고 비')
                ),
                precipitation_probability INTEGER NOT NULL CHECK (
                    precipitation_probability BETWEEN 0 AND 100
                ),
                forecast_score INTEGER NOT NULL CHECK (forecast_score BETWEEN 1 AND 4),
                created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (batch_id) REFERENCES forecast_batches(id) ON DELETE CASCADE,
                FOREIGN KEY (region_id) REFERENCES regions(id) ON DELETE CASCADE,
                UNIQUE (batch_id, region_id, forecast_date, time_period)
            );

            CREATE INDEX IF NOT EXISTS idx_forecasts_region_date
            ON forecasts (region_id, forecast_date, time_period);

            CREATE INDEX IF NOT EXISTS idx_forecasts_batch
            ON forecasts (batch_id);
            """
        )


def _reset_legacy_schema_if_needed(connection: sqlite3.Connection) -> None:
    existing_tables = {
        row["name"]
        for row in connection.execute(
            "SELECT name FROM sqlite_master WHERE type = 'table'"
        ).fetchall()
    }
    if "forecasts" not in existing_tables:
        return

    columns = {
        row["name"]
        for row in connection.execute("PRAGMA table_info(forecasts)").fetchall()
    }
    expected_columns = {
        "id",
        "batch_id",
        "region_id",
        "forecast_date",
        "time_period",
        "forecast_label",
        "precipitation_probability",
        "forecast_score",
        "created_at",
    }
    legacy_columns = {"region_name"}

    if legacy_columns.issubset(columns) or not expected_columns.issubset(columns):
        connection.executescript(
            """
            DROP TABLE IF EXISTS forecasts;
            DROP TABLE IF EXISTS forecast_batches;
            DROP TABLE IF EXISTS regions;
            DROP TABLE IF EXISTS users;
            """
        )


def seed_forecast_data() -> dict[str, int]:
    if not CSV_PATH.exists():
        return {"seeded_rows": 0, "seeded_batches": 0}

    dataframe = pd.read_csv(CSV_PATH, encoding="cp949")

    with get_connection() as connection:
        existing_count = connection.execute(
            "SELECT COUNT(*) AS count FROM forecasts"
        ).fetchone()["count"]
        if existing_count > 0:
            return {"seeded_rows": 0, "seeded_batches": 0}

        for region_name, metadata in REGION_METADATA.items():
            connection.execute(
                """
                INSERT OR IGNORE INTO regions (code, name, latitude, longitude)
                VALUES (?, ?, ?, ?)
                """,
                (
                    metadata["code"],
                    region_name,
                    metadata["latitude"],
                    metadata["longitude"],
                ),
            )

        batch_ids = {}
        batch_counts = dataframe.groupby("발표시각").size().to_dict()
        for raw_published_at, row_count in batch_counts.items():
            published_at = _parse_published_at(raw_published_at)
            cursor = connection.execute(
                """
                INSERT OR IGNORE INTO forecast_batches (
                    raw_published_at, published_at, source_file, row_count
                )
                VALUES (?, ?, ?, ?)
                """,
                (raw_published_at, published_at, CSV_PATH.name, int(row_count)),
            )
            if cursor.lastrowid:
                batch_ids[raw_published_at] = cursor.lastrowid
            else:
                batch_ids[raw_published_at] = connection.execute(
                    "SELECT id FROM forecast_batches WHERE raw_published_at = ?",
                    (raw_published_at,),
                ).fetchone()["id"]

        region_ids = {
            row["name"]: row["id"]
            for row in connection.execute("SELECT id, name FROM regions").fetchall()
        }

        rows = []
        for record in dataframe.to_dict(orient="records"):
            forecast_date, time_period = _parse_forecast_at(record["예보시각"])
            rows.append(
                (
                    batch_ids[record["발표시각"]],
                    region_ids[record["지역"]],
                    forecast_date,
                    time_period,
                    record["예보"],
                    int(record["강수확률(%)"]),
                    FORECAST_SCORES[record["예보"]],
                )
            )

        connection.executemany(
            """
            INSERT OR IGNORE INTO forecasts (
                batch_id,
                region_id,
                forecast_date,
                time_period,
                forecast_label,
                precipitation_probability,
                forecast_score
            )
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            rows,
        )

    return {"seeded_rows": len(rows), "seeded_batches": len(batch_ids)}


def _parse_published_at(raw_published_at: str) -> str:
    return datetime.strptime(raw_published_at, "%Y-%m-%d %H시").isoformat()


def _parse_forecast_at(raw_forecast_at: str) -> tuple[str, str]:
    forecast_date, period = raw_forecast_at.split()
    return forecast_date, TIME_PERIODS[period]
