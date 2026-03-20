from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from backend.api.routes.auth import router as auth_router
from backend.api.routes.forecasts import router as forecasts_router
from backend.api.routes.regions import router as regions_router
from backend.db.database import init_db, seed_forecast_data

openapi_tags = [
    {
        "name": "auth",
        "description": "JWT authentication endpoints.",
    },
    {
        "name": "health",
        "description": "Backend health check endpoints.",
    },
    {
        "name": "regions",
        "description": "Region reference endpoints.",
    },
    {
        "name": "forecasts",
        "description": "Forecast CRUD and map endpoints.",
    },
]


@asynccontextmanager
async def lifespan(_: FastAPI):
    init_db()
    seed_forecast_data()
    yield


app = FastAPI(
    title="Weather Map Backend",
    version="0.1.0",
    description="Backend API for the public weather forecast map project.",
    openapi_tags=openapi_tags,
    docs_url="/docs",
    redoc_url="/redoc",
    openapi_url="/openapi.json",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(auth_router)
app.include_router(regions_router)
app.include_router(forecasts_router)


@app.get("/", tags=["health"])
def read_root() -> dict[str, str]:
    return {"message": "Weather Map Backend is running.", "docs": "/docs"}
