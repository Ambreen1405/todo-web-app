import pytest
from fastapi.testclient import TestClient
from sqlmodel import Session
from unittest.mock import patch
from datetime import datetime
import os

from src.main import app
from src.models.user import User
from src.models.task import Task
from src.database.session import get_session


@pytest.fixture
def client():
    with TestClient(app) as c:
        yield c


def test_health_check(client: TestClient):
    """Test health check endpoint"""
    response = client.get("/health")
    assert response.status_code == 200
    data = response.json()
    assert data["status"] == "healthy"


def test_auth_registration_success(client: TestClient):
    """Test successful user registration"""
    response = client.post("/auth/register", json={
        "email": "test@example.com",
        "name": "Test User",
        "password": "password123"
    })
    assert response.status_code == 201

    data = response.json()
    assert data["email"] == "test@example.com"
    assert data["name"] == "Test User"
    assert "id" in data
    assert "hashed_password" not in data  # Should not expose hashed password


def test_auth_registration_duplicate_email(client: TestClient):
    """Test registration with duplicate email"""
    # Register user first
    client.post("/auth/register", json={
        "email": "duplicate@example.com",
        "name": "First User",
        "password": "password123"
    })

    # Try to register with same email
    response = client.post("/auth/register", json={
        "email": "duplicate@example.com",
        "name": "Second User",
        "password": "password123"
    })
    assert response.status_code == 400


def test_auth_login_success(client: TestClient):
    """Test successful login"""
    # Register user first
    client.post("/auth/register", json={
        "email": "login_test@example.com",
        "name": "Login Test",
        "password": "password123"
    })

    # Login with correct credentials
    response = client.post("/auth/login", json={
        "email": "login_test@example.com",
        "password": "password123"
    })
    assert response.status_code == 200

    data = response.json()
    assert "access_token" in data
    assert data["token_type"] == "bearer"


def test_auth_login_invalid_credentials(client: TestClient):
    """Test login with invalid credentials"""
    # Register user first
    client.post("/auth/register", json={
        "email": "invalid_login@example.com",
        "name": "Invalid Login",
        "password": "password123"
    })

    # Login with wrong password
    response = client.post("/auth/login", json={
        "email": "invalid_login@example.com",
        "password": "wrongpassword"
    })
    assert response.status_code == 401


def test_auth_me_endpoint_requires_auth(client: TestClient):
    """Test that /auth/me endpoint requires authentication"""
    # Try to access without token
    response = client.get("/auth/me")
    assert response.status_code == 401

    # Register and login to get a token
    client.post("/auth/register", json={
        "email": "me_test@example.com",
        "name": "ME Test",
        "password": "password123"
    })

    login_response = client.post("/auth/login", json={
        "email": "me_test@example.com",
        "password": "password123"
    })
    token = login_response.json()["access_token"]

    # Access with valid token
    response = client.get("/auth/me", headers={
        "Authorization": f"Bearer {token}"
    })
    assert response.status_code == 200

    data = response.json()
    assert data["email"] == "me_test@example.com"


def test_task_crud_operations(client: TestClient):
    """Test complete CRUD cycle for tasks"""
    # Register and login user
    client.post("/auth/register", json={
        "email": "crud_test@example.com",
        "name": "CRUD Test",
        "password": "password123"
    })

    login_response = client.post("/auth/login", json={
        "email": "crud_test@example.com",
        "password": "password123"
    })
    token = login_response.json()["access_token"]

    auth_headers = {"Authorization": f"Bearer {token}"}

    # Create a task
    response = client.post("/tasks/", json={
        "title": "Test Task",
        "description": "Test Description",
        "priority": "medium"
    }, headers=auth_headers)
    assert response.status_code == 201

    task_data = response.json()
    task_id = task_data["id"]
    assert task_data["title"] == "Test Task"
    assert task_data["description"] == "Test Description"
    assert task_data["priority"] == "medium"
    assert task_data["completed"] is False

    # Get the task
    response = client.get(f"/tasks/{task_id}", headers=auth_headers)
    assert response.status_code == 200

    retrieved_task = response.json()
    assert retrieved_task["id"] == task_id
    assert retrieved_task["title"] == "Test Task"

    # Update the task
    response = client.put(f"/tasks/{task_id}", json={
        "title": "Updated Task",
        "completed": True
    }, headers=auth_headers)
    assert response.status_code == 200

    updated_task = response.json()
    assert updated_task["id"] == task_id
    assert updated_task["title"] == "Updated Task"
    assert updated_task["completed"] is True

    # Get all tasks (should include our task)
    response = client.get("/tasks/", headers=auth_headers)
    assert response.status_code == 200

    tasks = response.json()
    assert len(tasks) == 1
    assert tasks[0]["id"] == task_id
    assert tasks[0]["title"] == "Updated Task"

    # Delete the task
    response = client.delete(f"/tasks/{task_id}", headers=auth_headers)
    assert response.status_code == 204  # No content

    # Verify task is deleted
    response = client.get(f"/tasks/{task_id}", headers=auth_headers)
    assert response.status_code == 404


def test_task_filtering(client: TestClient):
    """Test task filtering functionality"""
    # Register and login user
    client.post("/auth/register", json={
        "email": "filter_test@example.com",
        "name": "Filter Test",
        "password": "password123"
    })

    login_response = client.post("/auth/login", json={
        "email": "filter_test@example.com",
        "password": "password123"
    })
    token = login_response.json()["access_token"]

    auth_headers = {"Authorization": f"Bearer {token}"}

    # Create multiple tasks with different properties
    client.post("/tasks/", json={
        "title": "Completed Low Priority",
        "description": "Task 1",
        "priority": "low",
        "completed": True
    }, headers=auth_headers)

    client.post("/tasks/", json={
        "title": "Pending High Priority",
        "description": "Task 2",
        "priority": "high",
        "completed": False
    }, headers=auth_headers)

    client.post("/tasks/", json={
        "title": "Pending Medium Priority",
        "description": "Task 3",
        "priority": "medium",
        "completed": False
    }, headers=auth_headers)

    # Test filtering by completion status
    response = client.get("/tasks/?completed=true", headers=auth_headers)
    completed_tasks = response.json()
    assert len(completed_tasks) == 1
    assert completed_tasks[0]["completed"] is True

    response = client.get("/tasks/?completed=false", headers=auth_headers)
    pending_tasks = response.json()
    assert len(pending_tasks) == 2
    for task in pending_tasks:
        assert task["completed"] is False

    # Test filtering by priority
    response = client.get("/tasks/?priority=high", headers=auth_headers)
    high_priority_tasks = response.json()
    assert len(high_priority_tasks) == 1
    assert high_priority_tasks[0]["priority"] == "high"

    # Test filtering by both completion and priority
    response = client.get("/tasks/?completed=false&priority=medium", headers=auth_headers)
    filtered_tasks = response.json()
    assert len(filtered_tasks) == 1
    assert filtered_tasks[0]["completed"] is False
    assert filtered_tasks[0]["priority"] == "medium"


def test_cross_user_data_isolation(client: TestClient):
    """Test that users cannot access each other's data"""
    # Register two users
    client.post("/auth/register", json={
        "email": "user1@example.com",
        "name": "User 1",
        "password": "password123"
    })

    client.post("/auth/register", json={
        "email": "user2@example.com",
        "name": "User 2",
        "password": "password123"
    })

    # Login as user1
    login_response1 = client.post("/auth/login", json={
        "email": "user1@example.com",
        "password": "password123"
    })
    user1_token = login_response1.json()["access_token"]

    # Login as user2
    login_response2 = client.post("/auth/login", json={
        "email": "user2@example.com",
        "password": "password123"
    })
    user2_token = login_response2.json()["access_token"]

    user1_headers = {"Authorization": f"Bearer {user1_token}"}
    user2_headers = {"Authorization": f"Bearer {user2_token}"}

    # User1 creates a task
    response = client.post("/tasks/", json={
        "title": "User 1's Private Task",
        "description": "This belongs to user 1",
        "priority": "high"
    }, headers=user1_headers)
    assert response.status_code == 201
    user1_task_id = response.json()["id"]

    # User2 creates a task
    response = client.post("/tasks/", json={
        "title": "User 2's Private Task",
        "description": "This belongs to user2",
        "priority": "low"
    }, headers=user2_headers)
    assert response.status_code == 201
    user2_task_id = response.json()["id"]

    # Verify user1 can only see their own task
    response = client.get("/tasks/", headers=user1_headers)
    user1_tasks = response.json()
    assert len(user1_tasks) == 1
    assert user1_tasks[0]["id"] == user1_task_id
    assert user1_tasks[0]["title"] == "User 1's Private Task"

    # Verify user2 can only see their own task
    response = client.get("/tasks/", headers=user2_headers)
    user2_tasks = response.json()
    assert len(user2_tasks) == 1
    assert user2_tasks[0]["id"] == user2_task_id
    assert user2_tasks[0]["title"] == "User 2's Private Task"

    # Verify user2 cannot access user1's task directly
    response = client.get(f"/tasks/{user1_task_id}", headers=user2_headers)
    assert response.status_code == 404  # Task not found for this user

    # Verify user1 cannot access user2's task directly
    response = client.get(f"/tasks/{user2_task_id}", headers=user1_headers)
    assert response.status_code == 404  # Task not found for this user

    # Verify user2 cannot update user1's task
    response = client.put(f"/tasks/{user1_task_id}", json={
        "title": "Hacked by User 2"
    }, headers=user2_headers)
    assert response.status_code == 404

    # Verify user2 cannot delete user1's task
    response = client.delete(f"/tasks/{user1_task_id}", headers=user2_headers)
    assert response.status_code == 404