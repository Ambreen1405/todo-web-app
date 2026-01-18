from datetime import datetime, timedelta
from typing import Optional
import jwt
from fastapi import HTTPException, status
from sqlalchemy.orm import Session

from ..config import settings
from ..models.user import User
from ..schemas.token import TokenData


def create_access_token(data: dict, expires_delta: Optional[timedelta] = None):
    """
    Create a new access token with the provided data and expiration time
    """
    to_encode = data.copy()
    if expires_delta:
        expire = datetime.utcnow() + expires_delta
    else:
        expire = datetime.utcnow() + timedelta(minutes=settings.access_token_expire_minutes)

    to_encode.update({"exp": expire})
    encoded_jwt = jwt.encode(to_encode, settings.secret_key, algorithm=settings.algorithm)
    return encoded_jwt


def verify_token(token: str, credentials_exception):
    """
    Verify the provided token and return the username if valid
    """
    try:
        payload = jwt.decode(token, settings.secret_key, algorithms=[settings.algorithm])
        username: str = payload.get("sub")
        if username is None:
            raise credentials_exception
        token_data = TokenData(username=username)
    except jwt.ExpiredSignatureError:
        raise credentials_exception
    except jwt.InvalidTokenError:
        raise credentials_exception
    return token_data


from fastapi import Depends
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from ..database.session import get_session

security = HTTPBearer()

def get_current_user_from_token(token: str, db: Session):
    """
    Get the current user based on the provided token
    """
    credentials_exception = HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Could not validate credentials",
        headers={"WWW-Authenticate": "Bearer"},
    )

    token_data = verify_token(token, credentials_exception)

    user = db.query(User).filter(User.email == token_data.username).first()
    if user is None:
        raise credentials_exception

    return user


def get_current_user(
    credentials: HTTPAuthorizationCredentials = Depends(security),
    db: Session = Depends(get_session)
):
    """
    FastAPI dependency to get the current user based on the provided token
    """
    return get_current_user_from_token(credentials.credentials, db)