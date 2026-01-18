from fastapi import HTTPException, status, Depends
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from sqlmodel import Session
from jose import JWTError, jwt
from typing import Optional

from ..config import settings
from ..models.user import User
from ..database.session import get_session
from ..schemas.token import TokenData


security = HTTPBearer()


def verify_token(token: str) -> Optional[TokenData]:
    """
    Verify the provided JWT token and return the token data if valid
    """
    try:
        payload = jwt.decode(
            token,
            settings.secret_key,
            algorithms=[settings.algorithm]
        )
        username: str = payload.get("sub")

        if username is None:
            return None

        token_data = TokenData(username=username)
        return token_data

    except JWTError:
        return None


def get_current_user(
    credentials: HTTPAuthorizationCredentials = Depends(security),
    db: Session = Depends(get_session)
) -> User:
    """
    Get the current user based on the provided token
    """
    credentials_exception = HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Could not validate credentials",
        headers={"WWW-Authenticate": "Bearer"},
    )

    token_data = verify_token(credentials.credentials)

    if token_data is None:
        raise credentials_exception

    user = db.query(User).filter(User.email == token_data.username).first()

    if user is None:
        raise credentials_exception

    return user


def get_optional_user(
    credentials: HTTPAuthorizationCredentials = Depends(security),
    db: Session = Depends(get_session)
) -> Optional[User]:
    """
    Get the current user if token is provided, otherwise return None
    """
    try:
        token_data = verify_token(credentials.credentials)

        if token_data is None:
            return None

        user = db.query(User).filter(User.email == token_data.username).first()
        return user
    except:
        return None