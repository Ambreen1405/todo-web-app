from fastapi import APIRouter, Depends, HTTPException, status
from fastapi.security import OAuth2PasswordBearer
from sqlmodel import Session
from pydantic import BaseModel

from ..services.auth_service import register_user, login_user, logout_user
from ..schemas.user import UserCreate, UserResponse
from ..schemas.token import Token
from ..database.session import get_session
from ..models.user import User
from ..utils.auth import get_current_user


class UserLogin(BaseModel):
    email: str
    password: str

router = APIRouter(tags=["Authentication"])

oauth2_scheme = OAuth2PasswordBearer(tokenUrl="/auth/login")


@router.post("/auth/register", response_model=UserResponse, status_code=status.HTTP_201_CREATED)
def register(user_data: UserCreate, db: Session = Depends(get_session)):
    """
    Register a new user
    """
    try:
        # Log the received data for debugging
        from ..utils.logging import logger
        logger.info(f"Received registration data: email={user_data.email}, name={user_data.name}")
        db_user = register_user(user_data, db)
        return UserResponse.from_orm(db_user)
    except HTTPException:
        # Re-raise HTTP exceptions (like email already exists)
        raise
    except Exception as e:
        logger.error(f"Error in register endpoint: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="An error occurred during registration"
        )


@router.post("/auth/login", response_model=Token)
def login(user_credentials: UserLogin, db: Session = Depends(get_session)):
    """
    Login a user and return an access token
    """
    try:
        token = login_user(user_credentials.email, user_credentials.password, db)
        if not token:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Incorrect email or password",
                headers={"WWW-Authenticate": "Bearer"},
            )
        return token
    except HTTPException:
        # Re-raise HTTP exceptions
        raise
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="An error occurred during login"
        )


@router.post("/auth/logout")
def logout(token: str = Depends(oauth2_scheme)):
    """
    Logout a user
    """
    try:
        success = logout_user(token)
        if success:
            return {"message": "Successfully logged out"}
        else:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="An error occurred during logout"
            )
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="An error occurred during logout"
        )


@router.get("/auth/me", response_model=UserResponse)
def read_users_me(current_user: User = Depends(get_current_user)):
    """
    Get current user info
    """
    return UserResponse.from_orm(current_user)