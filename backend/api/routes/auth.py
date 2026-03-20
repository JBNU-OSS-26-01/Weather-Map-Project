import sqlite3

from fastapi import APIRouter, Depends, HTTPException, status

from backend.api.deps import get_current_user
from backend.core.security import create_access_token, hash_password, verify_password
from backend.db.database import get_connection
from backend.db.repositories import create_user, fetch_user_by_username
from backend.schemas.auth import TokenResponse, UserCreate, UserLogin, UserRead

router = APIRouter(prefix="/auth", tags=["auth"])


@router.post("/register", response_model=UserRead, status_code=status.HTTP_201_CREATED)
def register(payload: UserCreate) -> UserRead:
    with get_connection() as connection:
        existing_user = fetch_user_by_username(connection, payload.username)
        if existing_user is not None:
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail="Username already exists.",
            )

        try:
            user = create_user(
                connection,
                username=payload.username,
                email=payload.email,
                password_hash=hash_password(payload.password),
            )
        except sqlite3.IntegrityError as exc:
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail="User already exists.",
            ) from exc

    return UserRead.model_validate(dict(user))


@router.post("/login", response_model=TokenResponse)
def login(payload: UserLogin) -> TokenResponse:
    with get_connection() as connection:
        user = fetch_user_by_username(connection, payload.username)

    if user is None or not verify_password(payload.password, user["password_hash"]):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid username or password.",
        )

    user_read = UserRead.model_validate(
        {
            "id": user["id"],
            "username": user["username"],
            "email": user["email"],
            "is_active": user["is_active"],
            "created_at": user["created_at"],
        }
    )
    return TokenResponse(
        access_token=create_access_token(str(user["id"])),
        user=user_read,
    )


@router.get("/me", response_model=UserRead)
def read_current_user(current_user: dict = Depends(get_current_user)) -> UserRead:
    return UserRead.model_validate(current_user)
