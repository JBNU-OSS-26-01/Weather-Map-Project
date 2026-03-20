import sqlite3

from fastapi import APIRouter, Depends, HTTPException, Query, status

from backend.api.deps import get_current_user
from backend.db.database import FORECAST_SCORES, get_connection, seed_forecast_data
from backend.db.repositories import (
    create_forecast,
    delete_forecast,
    get_forecast,
    get_latest_batch_id,
    list_batches,
    list_forecasts,
)
from backend.db.repositories import update_forecast as repository_update_forecast
from backend.schemas.forecast import (
    BatchRead,
    ForecastCreate,
    ForecastOptionsRead,
    ForecastRead,
    ForecastUpdate,
    MapForecastRead,
)

router = APIRouter(prefix="/forecasts", tags=["forecasts"])

FORECAST_COLORS = {
    "맑음": ("#2ecc71", [46, 204, 113, 190]),
    "구름많음": ("#f1c40f", [241, 196, 15, 190]),
    "흐림": ("#7f8c8d", [127, 140, 141, 190]),
    "흐리고 비": ("#3498db", [52, 152, 219, 210]),
}


@router.get("/batches", response_model=list[BatchRead])
def read_batches() -> list[BatchRead]:
    with get_connection() as connection:
        rows = list_batches(connection)
    return [BatchRead.model_validate(dict(row)) for row in rows]


@router.post("/import-csv")
def import_csv(_: dict = Depends(get_current_user)) -> dict[str, int]:
    return seed_forecast_data()


@router.get("", response_model=list[ForecastRead])
def read_forecasts(
    batch_id: int | None = None,
    region_id: int | None = None,
    forecast_date: str | None = None,
    time_period: str | None = Query(default=None, pattern="^(AM|PM)$"),
) -> list[ForecastRead]:
    with get_connection() as connection:
        rows = list_forecasts(
            connection,
            batch_id=batch_id,
            region_id=region_id,
            forecast_date=forecast_date,
            time_period=time_period,
        )
    return [ForecastRead.model_validate(dict(row)) for row in rows]


@router.get("/options", response_model=ForecastOptionsRead)
def read_forecast_options() -> ForecastOptionsRead:
    with get_connection() as connection:
        latest_batch_id = get_latest_batch_id(connection)
        rows = list_forecasts(connection, batch_id=latest_batch_id)

    return ForecastOptionsRead(
        latest_batch_id=latest_batch_id,
        available_dates=sorted({row["forecast_date"] for row in rows}),
        available_time_periods=sorted({row["time_period"] for row in rows}),
    )


@router.get("/map", response_model=list[MapForecastRead])
def read_map_forecasts(
    forecast_date: str | None = None,
    time_period: str | None = Query(default=None, pattern="^(AM|PM)$"),
) -> list[MapForecastRead]:
    with get_connection() as connection:
        latest_batch_id = get_latest_batch_id(connection)
        rows = list_forecasts(
            connection,
            batch_id=latest_batch_id,
            forecast_date=forecast_date,
            time_period=time_period,
        )

    mapped_rows = []
    for row in rows:
        color_hex, color_rgba = FORECAST_COLORS[row["forecast_label"]]
        mapped_rows.append(
            MapForecastRead(
                region_id=row["region_id"],
                region_name=row["region_name"],
                region_code=row["region_code"],
                latitude=row["latitude"],
                longitude=row["longitude"],
                forecast_date=row["forecast_date"],
                time_period=row["time_period"],
                forecast_label=row["forecast_label"],
                precipitation_probability=row["precipitation_probability"],
                forecast_score=row["forecast_score"],
                color_hex=color_hex,
                color_rgba=color_rgba,
                published_at=row["published_at"],
            )
        )

    return mapped_rows


@router.get("/{forecast_id}", response_model=ForecastRead)
def read_forecast(forecast_id: int) -> ForecastRead:
    with get_connection() as connection:
        row = get_forecast(connection, forecast_id)

    if row is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Forecast not found.")
    return ForecastRead.model_validate(dict(row))


@router.post("", response_model=ForecastRead, status_code=status.HTTP_201_CREATED)
def create_forecast_record(
    payload: ForecastCreate,
    _: dict = Depends(get_current_user),
) -> ForecastRead:
    with get_connection() as connection:
        try:
            row = create_forecast(connection, payload.model_dump())
        except sqlite3.IntegrityError as exc:
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail="Forecast violates a database constraint.",
            ) from exc

    return ForecastRead.model_validate(dict(row))


@router.put("/{forecast_id}", response_model=ForecastRead)
def update_forecast_record(
    forecast_id: int,
    payload: ForecastUpdate,
    _: dict = Depends(get_current_user),
) -> ForecastRead:
    with get_connection() as connection:
        existing = get_forecast(connection, forecast_id)
        if existing is None:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Forecast not found.",
            )
        try:
            row = repository_update_forecast(connection, forecast_id, payload.model_dump())
        except sqlite3.IntegrityError as exc:
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail="Forecast violates a database constraint.",
            ) from exc

    return ForecastRead.model_validate(dict(row))


@router.delete("/{forecast_id}", status_code=status.HTTP_204_NO_CONTENT)
def delete_forecast_record(
    forecast_id: int,
    _: dict = Depends(get_current_user),
) -> None:
    with get_connection() as connection:
        existing = get_forecast(connection, forecast_id)
        if existing is None:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Forecast not found.",
            )
        delete_forecast(connection, forecast_id)
