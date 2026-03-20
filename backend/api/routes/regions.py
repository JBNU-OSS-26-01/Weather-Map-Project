from fastapi import APIRouter

from backend.db.database import get_connection
from backend.db.repositories import list_regions
from backend.schemas.forecast import RegionRead

router = APIRouter(prefix="/regions", tags=["regions"])


@router.get("", response_model=list[RegionRead])
def read_regions() -> list[RegionRead]:
    with get_connection() as connection:
        rows = list_regions(connection)
    return [RegionRead.model_validate(dict(row)) for row in rows]
