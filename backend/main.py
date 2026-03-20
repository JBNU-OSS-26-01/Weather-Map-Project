from fastapi import FastAPI

from backend.api.routes.weather import router as weather_router


app = FastAPI(
    title="Weather Map Backend",
    version="0.1.0",
    description="Backend API for the public weather forecast map project.",
)

app.include_router(weather_router)


@app.get("/", tags=["health"])
def read_root() -> dict[str, str]:
    return {"message": "Weather Map Backend is running."}
