from fastapi import APIRouter

router = APIRouter(prefix="/weather", tags=["weather"])


@router.get("/mid-forecast")
def get_mid_forecast() -> dict[str, str]:
    return {
        "message": "Mid-term forecast endpoint placeholder. Implementation will be added later."
    }
