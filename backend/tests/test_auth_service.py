import pytest
from sqlmodel import Session, select
from fastapi import HTTPException, status
from unittest.mock import Mock

from src.models.user import User
from src.schemas.user import UserCreate
from src.services.auth_service import register_user, authenticate_user, login_user
from src.utils.security import get_password_hash


def test_register_user_success(session: Session):
    """Test successful user registration"""
    user_data = UserCreate(
        email="test@example.com",
        name="Test User",
        password="password123"
    )

    # Register a new user
    result = register_user(user_data, session)

    # Verify the user was created
    assert result.email == user_data.email
    assert result.name == user_data.name
    assert result.hashed_password != user_data.password  # Password should be hashed
    # pbkdf2 hash format: salt$hash
    assert '$' in result.hashed_password  # pbkdf2 format contains a $ separator

    # Verify the user exists in the database
    retrieved_user = session.get(User, result.id)
    assert retrieved_user is not None
    assert retrieved_user.email == user_data.email


def test_register_user_duplicate_email(session: Session):
    """Test that registering a user with an existing email raises an exception"""
    user_data = UserCreate(
        email="duplicate@example.com",
        name="First User",
        password="password123"
    )

    # Register the first user
    register_user(user_data, session)

    # Try to register another user with the same email
    with pytest.raises(HTTPException) as exc_info:
        register_user(user_data, session)

    # Verify the exception details
    assert exc_info.value.status_code == status.HTTP_400_BAD_REQUEST
    assert "already exists" in exc_info.value.detail


def test_register_user_invalid_email(session: Session):
    """Test that registering a user with invalid email raises an exception"""
    user_data = UserCreate(
        email="invalid-email",  # Invalid email format
        name="Test User",
        password="password123"
    )

    with pytest.raises(HTTPException) as exc_info:
        register_user(user_data, session)

    assert exc_info.value.status_code == status.HTTP_400_BAD_REQUEST
    assert "valid email" in exc_info.value.detail


def test_register_user_short_password(session: Session):
    """Test that registering a user with a short password raises an exception"""
    user_data = UserCreate(
        email="test@example.com",
        name="Test User",
        password="short"  # Less than 8 characters
    )

    with pytest.raises(HTTPException) as exc_info:
        register_user(user_data, session)

    assert exc_info.value.status_code == status.HTTP_400_BAD_REQUEST
    assert "8 characters" in exc_info.value.detail


def test_authenticate_user_success(session: Session):
    """Test successful user authentication"""
    # Create a user first
    user_data = UserCreate(
        email="test@example.com",
        name="Test User",
        password="password123"
    )
    registered_user = register_user(user_data, session)

    # Authenticate with correct credentials
    authenticated_user = authenticate_user(
        user_data.email,
        user_data.password,
        session
    )

    # Verify authentication succeeded
    assert authenticated_user is not None
    assert authenticated_user.id == registered_user.id
    assert authenticated_user.email == user_data.email


def test_authenticate_user_wrong_password(session: Session):
    """Test authentication failure with wrong password"""
    # Create a user first
    user_data = UserCreate(
        email="test@example.com",
        name="Test User",
        password="password123"
    )
    register_user(user_data, session)

    # Try to authenticate with wrong password
    authenticated_user = authenticate_user(
        user_data.email,
        "wrongpassword",
        session
    )

    # Verify authentication failed
    assert authenticated_user is None


def test_authenticate_user_nonexistent_email(session: Session):
    """Test authentication failure with nonexistent email"""
    authenticated_user = authenticate_user(
        "nonexistent@example.com",
        "password123",
        session
    )

    # Verify authentication failed
    assert authenticated_user is None


def test_authenticate_user_missing_credentials(session: Session):
    """Test authentication with missing credentials"""
    # Test with empty email
    authenticated_user = authenticate_user("", "password", session)
    assert authenticated_user is None

    # Test with empty password
    authenticated_user = authenticate_user("test@example.com", "", session)
    assert authenticated_user is None


def test_login_user_success(session: Session):
    """Test successful user login"""
    # Create a user first
    user_data = UserCreate(
        email="test@example.com",
        name="Test User",
        password="password123"
    )
    register_user(user_data, session)

    # Login with correct credentials
    token_result = login_user(user_data.email, user_data.password, session)

    # Verify login succeeded
    assert token_result is not None
    assert hasattr(token_result, 'access_token')
    assert token_result.token_type == "bearer"
    assert len(token_result.access_token) > 0  # Token should not be empty


def test_login_user_failure(session: Session):
    """Test login failure with incorrect credentials"""
    # Create a user first
    user_data = UserCreate(
        email="test@example.com",
        name="Test User",
        password="password123"
    )
    register_user(user_data, session)

    # Try to login with wrong credentials
    with pytest.raises(HTTPException) as exc_info:
        login_user("test@example.com", "wrongpassword", session)

    # Verify the exception details
    assert exc_info.value.status_code == status.HTTP_401_UNAUTHORIZED
    assert "Incorrect" in exc_info.value.detail