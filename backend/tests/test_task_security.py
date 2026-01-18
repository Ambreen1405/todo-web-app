import pytest
from fastapi.testclient import TestClient

from src.main import app


@pytest.fixture
def client():
    with TestClient(app) as c:
        yield c


def test_user_cannot_access_other_users_tasks(client: TestClient):
    """
    Test that a user cannot access tasks belonging to another user
    """
    # Create two users
    user1_email = "user1@example.com"
    user1_password = "password123"
    user2_email = "user2@example.com"
    user2_password = "password123"

    # Register user1
    response = client.post("/auth/register", json={
        "email": user1_email,
        "name": "User 1",
        "password": user1_password
    })
    assert response.status_code == 201

    # Login as user1 to get token
    response = client.post("/auth/login", json={
        "email": user1_email,
        "password": user1_password
    })
    assert response.status_code == 200
    user1_token = response.json()["access_token"]

    # Create a task for user1
    response = client.post("/tasks/",
                          json={"title": "User 1 task", "description": "Task for user 1"},
                          headers={"Authorization": f"Bearer user1_token"})
    assert response.status_code == 201
    user1_task_id = response.json()["id"]

    # Register user2
    response = client.post("/auth/register", json={
        "email": user2_email,
        "name": "User 2",
        "password": user2_password
    })
    assert response.status_code == 201

    # Login as user2 to get token
    response = client.post("/auth/login", json={
        "email": user2_email,
        "password": user2_password
    })
    assert response.status_code == 200
    user2_token = response.json()["access_token"]

    # Try to access user1's task as user2 - should fail with 404
    response = client.get(f"/tasks/{user1_task_id}",
                         headers={"Authorization": f"Bearer {user2_token}"})
    assert response.status_code == 404  # Task not found for this user

    # Create a task for user2
    response = client.post("/tasks/",
                          json={"title": "User 2 task", "description": "Task for user 2"},
                          headers={"Authorization": f"Bearer user2_token"})
    assert response.status_code == 201
    user2_task_id = response.json()["id"]

    # Verify user1 can't access user2's task
    response = client.get(f"/tasks/{user2_task_id}",
                         headers={"Authorization": f"Bearer {user1_token}"})
    assert response.status_code == 404  # Task not found for this user

    # Verify each user can access their own tasks
    response = client.get(f"/tasks/{user1_task_id}",
                         headers={"Authorization": f"Bearer {user1_token}"})
    assert response.status_code == 200
    assert response.json()["id"] == user1_task_id

    response = client.get(f"/tasks/{user2_task_id}",
                         headers={"Authorization": f"Bearer {user2_token}"})
    assert response.status_code == 200
    assert response.json()["id"] == user2_task_id


def test_user_cannot_update_other_users_tasks(client: TestClient):
    """
    Test that a user cannot update tasks belonging to another user
    """
    # Create two users and their tasks
    user1_email = "user1@example.com"
    user1_password = "password123"
    user2_email = "user2@example.com"
    user2_password = "password123"

    # Register user1
    response = client.post("/auth/register", json={
        "email": user1_email,
        "name": "User 1",
        "password": user1_password
    })
    assert response.status_code == 201

    # Login as user1
    response = client.post("/auth/login", json={
        "email": user1_email,
        "password": user1_password
    })
    assert response.status_code == 200
    user1_token = response.json()["access_token"]

    # Create a task for user1
    response = client.post("/tasks/",
                          json={"title": "User 1 task", "description": "Task for user 1"},
                          headers={"Authorization": f"Bearer user1_token"})
    assert response.status_code == 201
    user1_task_id = response.json()["id"]

    # Register user2
    response = client.post("/auth/register", json={
        "email": user2_email,
        "name": "User 2",
        "password": user2_password
    })
    assert response.status_code == 201

    # Login as user2
    response = client.post("/auth/login", json={
        "email": user2_email,
        "password": user2_password
    })
    assert response.status_code == 200
    user2_token = response.json()["access_token"]

    # Try to update user1's task as user2 - should fail with 404
    response = client.put(f"/tasks/{user1_task_id}",
                         json={"title": "Modified by user 2"},
                         headers={"Authorization": f"Bearer {user2_token}"})
    assert response.status_code == 404  # Task not found for this user


def test_user_cannot_delete_other_users_tasks(client: TestClient):
    """
    Test that a user cannot delete tasks belonging to another user
    """
    # Create two users and their tasks
    user1_email = "user1@example.com"
    user1_password = "password123"
    user2_email = "user2@example.com"
    user2_password = "password123"

    # Register user1
    response = client.post("/auth/register", json={
        "email": user1_email,
        "name": "User 1",
        "password": user1_password
    })
    assert response.status_code == 201

    # Login as user1
    response = client.post("/auth/login", json={
        "email": user1_email,
        "password": user1_password
    })
    assert response.status_code == 200
    user1_token = response.json()["access_token"]

    # Create a task for user1
    response = client.post("/tasks/",
                          json={"title": "User 1 task", "description": "Task for user 1"},
                          headers={"Authorization": f"Bearer user1_token"})
    assert response.status_code == 201
    user1_task_id = response.json()["id"]

    # Register user2
    response = client.post("/auth/register", json={
        "email": user2_email,
        "name": "User 2",
        "password": user2_password
    })
    assert response.status_code == 201

    # Login as user2
    response = client.post("/auth/login", json={
        "email": user2_email,
        "password": user2_password
    })
    assert response.status_code == 200
    user2_token = response.json()["access_token"]

    # Try to delete user1's task as user2 - should fail with 404
    response = client.delete(f"/tasks/{user1_task_id}",
                           headers={"Authorization": f"Bearer {user2_token}"})
    assert response.status_code == 404  # Task not found for this user

    # Verify user1's task still exists
    response = client.get(f"/tasks/{user1_task_id}",
                         headers={"Authorization": f"Bearer {user1_token}"})
    assert response.status_code == 200


if __name__ == "__main__":
    pytest.main([__file__])