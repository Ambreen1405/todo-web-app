from sqlmodel import Session, select
from fastapi import HTTPException, status
from datetime import timedelta
from typing import Optional

from ..models.user import User
from ..schemas.user import UserCreate
from ..utils.security import verify_password, get_password_hash
from ..utils.auth import create_access_token
from ..schemas.token import Token
from ..utils.logging import logger


def register_user(user_data: UserCreate, db: Session) -> User:
    """
    Register a new user with the provided data
    """
    try:
        # Check if user with email already exists
        existing_user = db.exec(select(User).where(User.email == user_data.email)).first()
        if existing_user:
            logger.warning(f"Registration attempted with existing email: {user_data.email}")
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="A user with this email already exists"
            )

        # Validate user data
        if not user_data.email or '@' not in user_data.email:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="A valid email is required"
            )

        if not user_data.name or len(user_data.name.strip()) < 1:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="A valid name is required"
            )

        if len(user_data.password) < 8:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Password must be at least 8 characters long"
            )

        # Create new user with hashed password
        try:
            logger.info(f"Attempting to hash password for user: {user_data.email}")
            logger.info(f"Password length: {len(user_data.password)}")
            hashed_pwd = get_password_hash(user_data.password)  # Use the security utility function
            logger.info(f"Password hashed successfully")
        except Exception as e:
            logger.error(f"Error hashing password: {str(e)}")
            logger.error(f"Error type: {type(e)}")
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"An error occurred while processing your password: {str(e)}"
            )

        db_user = User(
            email=user_data.email,
            name=user_data.name,
            hashed_password=hashed_pwd
        )

        db.add(db_user)
        db.commit()
        db.refresh(db_user)

        logger.info(f"New user registered: {db_user.email}")
        return db_user
    except HTTPException:
        # Re-raise HTTP exceptions
        raise
    except Exception as e:
        logger.error(f"Unexpected error during user registration: {str(e)}", exc_info=True)  # Include full traceback
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"An unexpected error occurred during registration: {str(e)}"
        )


def authenticate_user(email: str, password: str, db: Session) -> Optional[User]:
    """
    Authenticate a user with the provided email and password
    """
    try:
        # Validate inputs
        if not email or not password:
            logger.warning(f"Login attempt with missing credentials for email: {email}")
            return None

        user = db.exec(select(User).where(User.email == email)).first()

        if not user:
            logger.warning(f"Login attempt for non-existent email: {email}")
            return None

        if not verify_password(password, user.hashed_password):
            logger.warning(f"Failed login attempt for email: {email} - invalid password")
            return None

        logger.info(f"Successful login for user: {user.email}")
        return user
    except Exception as e:
        logger.error(f"Unexpected error during authentication: {str(e)}")
        return None


def login_user(email: str, password: str, db: Session) -> Optional[Token]:
    """
    Login a user and return an access token
    """
    try:
        user = authenticate_user(email, password, db)

        if not user:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Incorrect email or password",
                headers={"WWW-Authenticate": "Bearer"},
            )

        # Create access token
        access_token_expires = timedelta(minutes=15)  # Could come from settings
        access_token = create_access_token(
            data={"sub": user.email},
            expires_delta=access_token_expires
        )

        logger.info(f"User {user.email} successfully logged in")
        return Token(access_token=access_token, token_type="bearer")
    except HTTPException:
        # Re-raise HTTP exceptions
        raise
    except Exception as e:
        logger.error(f"Unexpected error during login: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="An unexpected error occurred during login"
        )


def logout_user(token: str) -> bool:
    """
    Logout a user (currently just a placeholder - in a real app you'd blacklist the token)
    """
    try:
        # In a real application, you would implement token blacklisting here
        # For now, we just return success
        logger.info("User logged out")
        return True
    except Exception as e:
        logger.error(f"Unexpected error during logout: {str(e)}")
        return False