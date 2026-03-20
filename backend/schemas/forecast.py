from pydantic import BaseModel, Field


class BatchRead(BaseModel):
    id: int
    raw_published_at: str
    published_at: str
    source_file: str
    row_count: int
    imported_at: str


class RegionRead(BaseModel):
    id: int
    code: str
    name: str
    latitude: float
    longitude: float
    created_at: str


class ForecastBase(BaseModel):
    batch_id: int
    region_id: int
    forecast_date: str
    time_period: str = Field(pattern="^(AM|PM)$")
    forecast_label: str = Field(pattern="^(맑음|구름많음|흐림|흐리고 비)$")
    precipitation_probability: int = Field(ge=0, le=100)
    forecast_score: int = Field(ge=1, le=4)


class ForecastCreate(ForecastBase):
    pass


class ForecastUpdate(ForecastBase):
    pass


class ForecastRead(ForecastBase):
    id: int
    raw_published_at: str
    published_at: str
    region_name: str
    region_code: str
    latitude: float
    longitude: float
    created_at: str


class MapForecastRead(BaseModel):
    region_id: int
    region_name: str
    region_code: str
    latitude: float
    longitude: float
    forecast_date: str
    time_period: str
    forecast_label: str
    precipitation_probability: int
    forecast_score: int
    color_hex: str
    color_rgba: list[int]
    published_at: str


class ForecastOptionsRead(BaseModel):
    latest_batch_id: int | None
    available_dates: list[str]
    available_time_periods: list[str]
