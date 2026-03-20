import sqlite3
from typing import Any


def fetch_user_by_username(connection: sqlite3.Connection, username: str) -> sqlite3.Row | None:
    return connection.execute(
        "SELECT * FROM users WHERE username = ?",
        (username,),
    ).fetchone()


def fetch_user_by_id(connection: sqlite3.Connection, user_id: int) -> sqlite3.Row | None:
    return connection.execute(
        "SELECT id, username, email, is_active, created_at FROM users WHERE id = ?",
        (user_id,),
    ).fetchone()


def create_user(
    connection: sqlite3.Connection,
    username: str,
    email: str,
    password_hash: str,
) -> sqlite3.Row:
    cursor = connection.execute(
        """
        INSERT INTO users (username, email, password_hash)
        VALUES (?, ?, ?)
        """,
        (username, email, password_hash),
    )
    return fetch_user_by_id(connection, cursor.lastrowid)


def list_regions(connection: sqlite3.Connection) -> list[sqlite3.Row]:
    return connection.execute(
        """
        SELECT id, code, name, latitude, longitude, created_at
        FROM regions
        ORDER BY name
        """
    ).fetchall()


def list_batches(connection: sqlite3.Connection) -> list[sqlite3.Row]:
    return connection.execute(
        """
        SELECT id, raw_published_at, published_at, source_file, row_count, imported_at
        FROM forecast_batches
        ORDER BY published_at DESC
        """
    ).fetchall()


def get_latest_batch_id(connection: sqlite3.Connection) -> int | None:
    row = connection.execute(
        "SELECT id FROM forecast_batches ORDER BY published_at DESC LIMIT 1"
    ).fetchone()
    return row["id"] if row else None


def list_forecasts(
    connection: sqlite3.Connection,
    *,
    batch_id: int | None = None,
    region_id: int | None = None,
    forecast_date: str | None = None,
    time_period: str | None = None,
) -> list[sqlite3.Row]:
    query = """
        SELECT
            f.id,
            f.batch_id,
            fb.raw_published_at,
            fb.published_at,
            r.id AS region_id,
            r.name AS region_name,
            r.code AS region_code,
            r.latitude,
            r.longitude,
            f.forecast_date,
            f.time_period,
            f.forecast_label,
            f.precipitation_probability,
            f.forecast_score,
            f.created_at
        FROM forecasts f
        JOIN regions r ON r.id = f.region_id
        JOIN forecast_batches fb ON fb.id = f.batch_id
        WHERE 1 = 1
    """
    params: list[Any] = []

    if batch_id is not None:
        query += " AND f.batch_id = ?"
        params.append(batch_id)
    if region_id is not None:
        query += " AND f.region_id = ?"
        params.append(region_id)
    if forecast_date is not None:
        query += " AND f.forecast_date = ?"
        params.append(forecast_date)
    if time_period is not None:
        query += " AND f.time_period = ?"
        params.append(time_period)

    query += " ORDER BY fb.published_at DESC, f.forecast_date, f.time_period, r.name"
    return connection.execute(query, params).fetchall()


def get_forecast(connection: sqlite3.Connection, forecast_id: int) -> sqlite3.Row | None:
    rows = connection.execute(
        """
        SELECT
            f.id,
            f.batch_id,
            fb.raw_published_at,
            fb.published_at,
            r.id AS region_id,
            r.name AS region_name,
            r.code AS region_code,
            r.latitude,
            r.longitude,
            f.forecast_date,
            f.time_period,
            f.forecast_label,
            f.precipitation_probability,
            f.forecast_score,
            f.created_at
        FROM forecasts f
        JOIN regions r ON r.id = f.region_id
        JOIN forecast_batches fb ON fb.id = f.batch_id
        WHERE f.id = ?
        """,
        (forecast_id,),
    ).fetchone()
    return rows


def create_forecast(connection: sqlite3.Connection, payload: dict[str, Any]) -> sqlite3.Row:
    cursor = connection.execute(
        """
        INSERT INTO forecasts (
            batch_id,
            region_id,
            forecast_date,
            time_period,
            forecast_label,
            precipitation_probability,
            forecast_score
        )
        VALUES (
            :batch_id,
            :region_id,
            :forecast_date,
            :time_period,
            :forecast_label,
            :precipitation_probability,
            :forecast_score
        )
        """,
        payload,
    )
    return get_forecast(connection, cursor.lastrowid)


def update_forecast(
    connection: sqlite3.Connection,
    forecast_id: int,
    payload: dict[str, Any],
) -> sqlite3.Row | None:
    connection.execute(
        """
        UPDATE forecasts
        SET
            batch_id = :batch_id,
            region_id = :region_id,
            forecast_date = :forecast_date,
            time_period = :time_period,
            forecast_label = :forecast_label,
            precipitation_probability = :precipitation_probability,
            forecast_score = :forecast_score
        WHERE id = :forecast_id
        """,
        {**payload, "forecast_id": forecast_id},
    )
    return get_forecast(connection, forecast_id)


def delete_forecast(connection: sqlite3.Connection, forecast_id: int) -> None:
    connection.execute("DELETE FROM forecasts WHERE id = ?", (forecast_id,))
