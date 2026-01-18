import pytest
from sqlmodel import Session
from fastapi import HTTPException, status
from datetime import datetime

from src.models.user import User
from src.schemas.user import UserCreate
from src.models.task import Task, TaskCreate, TaskUpdate
from src.services.auth_service import register_user
from src.services.task_service import create_task, get_tasks, get_task_by_id, update_task, delete_task


def test_create_task_success(session: Session):
    """Test successful task creation"""
    # Create a user first
    user_data = UserCreate(
        email="test@example.com",
        name="Test User",
        password="password123"
    )
    user = register_user(user_data, session)

    # Create a task
    task_data = TaskCreate(
        title="Test Task",
        description="Test Description",
        priority="medium"
    )
    created_task = create_task(task_data, user.id, session)

    # Verify the task was created correctly
    assert created_task.title == task_data.title
    assert created_task.description == task_data.description
    assert created_task.priority == task_data.priority
    assert created_task.user_id == user.id
    assert created_task.completed is False  # Default value
    assert isinstance(created_task.created_at, datetime)
    assert isinstance(created_task.updated_at, datetime)


def test_create_task_missing_title(session: Session):
    """Test that creating a task without a title raises an exception"""
    # Create a user first
    user_data = UserCreate(
        email="test@example.com",
        name="Test User",
        password="password123"
    )
    user = register_user(user_data, session)

    # Try to create a task with empty title
    task_data = TaskCreate(
        title="",  # Empty title
        description="Test Description",
        priority="medium"
    )

    with pytest.raises(HTTPException) as exc_info:
        create_task(task_data, user.id, session)

    assert exc_info.value.status_code == status.HTTP_400_BAD_REQUEST
    assert "required" in exc_info.value.detail


def test_create_task_long_title(session: Session):
    """Test that creating a task with a too-long title raises an exception"""
    # Create a user first
    user_data = UserCreate(
        email="test@example.com",
        name="Test User",
        password="password123"
    )
    user = register_user(user_data, session)

    # Try to create a task with a very long title
    task_data = TaskCreate(
        title="a" * 300,  # Too long
        description="Test Description",
        priority="medium"
    )

    with pytest.raises(HTTPException) as exc_info:
        create_task(task_data, user.id, session)

    assert exc_info.value.status_code == status.HTTP_400_BAD_REQUEST
    assert "255 characters" in exc_info.value.detail


def test_create_task_invalid_priority(session: Session):
    """Test that creating a task with invalid priority raises an exception"""
    # Create a user first
    user_data = UserCreate(
        email="test@example.com",
        name="Test User",
        password="password123"
    )
    user = register_user(user_data, session)

    # Try to create a task with invalid priority
    task_data = TaskCreate(
        title="Test Task",
        description="Test Description",
        priority="invalid_priority"  # Not in allowed values
    )

    with pytest.raises(HTTPException) as exc_info:
        create_task(task_data, user.id, session)

    assert exc_info.value.status_code == status.HTTP_400_BAD_REQUEST
    assert "low, medium, high" in exc_info.value.detail


def test_get_tasks_for_user(session: Session):
    """Test getting tasks for a specific user"""
    # Create two users
    user1_data = UserCreate(email="user1@example.com", name="User 1", password="password123")
    user2_data = UserCreate(email="user2@example.com", name="User 2", password="password123")
    user1 = register_user(user1_data, session)
    user2 = register_user(user2_data, session)

    # Create tasks for user1
    task1_data = TaskCreate(title="User 1 Task 1", description="Desc 1", priority="low")
    task2_data = TaskCreate(title="User 1 Task 2", description="Desc 2", priority="high")
    create_task(task1_data, user1.id, session)
    create_task(task2_data, user1.id, session)

    # Create tasks for user2
    task3_data = TaskCreate(title="User 2 Task 1", description="Desc 3", priority="medium")
    create_task(task3_data, user2.id, session)

    # Get tasks for user1
    user1_tasks = get_tasks(user1.id, session)

    # Verify user1 only gets their own tasks
    assert len(user1_tasks) == 2
    for task in user1_tasks:
        assert task.user_id == user1.id

    # Get tasks for user2
    user2_tasks = get_tasks(user2.id, session)

    # Verify user2 only gets their own tasks
    assert len(user2_tasks) == 1
    assert user2_tasks[0].user_id == user2.id


def test_get_task_by_id_success(session: Session):
    """Test getting a specific task by ID"""
    # Create a user and task
    user_data = UserCreate(email="test@example.com", name="Test User", password="password123")
    user = register_user(user_data, session)

    task_data = TaskCreate(title="Test Task", description="Test Desc", priority="medium")
    created_task = create_task(task_data, user.id, session)

    # Get the task by ID
    retrieved_task = get_task_by_id(created_task.id, user.id, session)

    # Verify the task was retrieved correctly
    assert retrieved_task is not None
    assert retrieved_task.id == created_task.id
    assert retrieved_task.title == created_task.title


def test_get_task_by_id_not_found(session: Session):
    """Test getting a non-existent task returns None"""
    # Create a user
    user_data = UserCreate(email="test@example.com", name="Test User", password="password123")
    user = register_user(user_data, session)

    # Try to get a non-existent task
    retrieved_task = get_task_by_id(999, user.id, session)

    # Verify None is returned
    assert retrieved_task is None


def test_get_task_by_id_different_user(session: Session):
    """Test that one user cannot access another user's task"""
    # Create two users
    user1_data = UserCreate(email="user1@example.com", name="User 1", password="password123")
    user2_data = UserCreate(email="user2@example.com", name="User 2", password="password123")
    user1 = register_user(user1_data, session)
    user2 = register_user(user2_data, session)

    # Create a task for user1
    task_data = TaskCreate(title="User 1 Task", description="Test Desc", priority="medium")
    created_task = create_task(task_data, user1.id, session)

    # Try to get user1's task as user2
    retrieved_task = get_task_by_id(created_task.id, user2.id, session)

    # Verify None is returned (user2 cannot access user1's task)
    assert retrieved_task is None


def test_update_task_success(session: Session):
    """Test successful task update"""
    # Create a user and task
    user_data = UserCreate(email="test@example.com", name="Test User", password="password123")
    user = register_user(user_data, session)

    task_data = TaskCreate(title="Original Title", description="Original Desc", priority="low")
    created_task = create_task(task_data, user.id, session)

    # Update the task
    update_data = TaskUpdate(
        title="Updated Title",
        description="Updated Description",
        priority="high",
        completed=True
    )
    updated_task = update_task(created_task.id, update_data, user.id, session)

    # Verify the task was updated correctly
    assert updated_task is not None
    assert updated_task.title == update_data.title
    assert updated_task.description == update_data.description
    assert updated_task.priority == update_data.priority
    assert updated_task.completed == update_data.completed
    # Updated time should be newer than created time
    assert updated_task.updated_at >= updated_task.created_at


def test_update_task_not_found(session: Session):
    """Test updating a non-existent task"""
    # Create a user
    user_data = UserCreate(email="test@example.com", name="Test User", password="password123")
    user = register_user(user_data, session)

    # Try to update a non-existent task
    update_data = TaskUpdate(title="Updated Title")
    result = update_task(999, update_data, user.id, session)

    # Verify None is returned
    assert result is None


def test_update_task_different_user(session: Session):
    """Test that one user cannot update another user's task"""
    # Create two users
    user1_data = UserCreate(email="user1@example.com", name="User 1", password="password123")
    user2_data = UserCreate(email="user2@example.com", name="User 2", password="password123")
    user1 = register_user(user1_data, session)
    user2 = register_user(user2_data, session)

    # Create a task for user1
    task_data = TaskCreate(title="User 1 Task", description="Test Desc", priority="medium")
    created_task = create_task(task_data, user1.id, session)

    # Try to update user1's task as user2
    update_data = TaskUpdate(title="Hacked Title")
    result = update_task(created_task.id, update_data, user2.id, session)

    # Verify None is returned (update failed)
    assert result is None

    # Verify the original task wasn't changed
    original_task = get_task_by_id(created_task.id, user1.id, session)
    assert original_task.title == "User 1 Task"


def test_delete_task_success(session: Session):
    """Test successful task deletion"""
    # Create a user and task
    user_data = UserCreate(email="test@example.com", name="Test User", password="password123")
    user = register_user(user_data, session)

    task_data = TaskCreate(title="Test Task", description="Test Desc", priority="medium")
    created_task = create_task(task_data, user.id, session)

    # Verify task exists before deletion
    existing_task = get_task_by_id(created_task.id, user.id, session)
    assert existing_task is not None

    # Delete the task
    success = delete_task(created_task.id, user.id, session)

    # Verify deletion was successful
    assert success is True

    # Verify task no longer exists
    deleted_task = get_task_by_id(created_task.id, user.id, session)
    assert deleted_task is None


def test_delete_task_not_found(session: Session):
    """Test deleting a non-existent task"""
    # Create a user
    user_data = UserCreate(email="test@example.com", name="Test User", password="password123")
    user = register_user(user_data, session)

    # Try to delete a non-existent task
    success = delete_task(999, user.id, session)

    # Verify deletion failed
    assert success is False


def test_delete_task_different_user(session: Session):
    """Test that one user cannot delete another user's task"""
    # Create two users
    user1_data = UserCreate(email="user1@example.com", name="User 1", password="password123")
    user2_data = UserCreate(email="user2@example.com", name="User 2", password="password123")
    user1 = register_user(user1_data, session)
    user2 = register_user(user2_data, session)

    # Create a task for user1
    task_data = TaskCreate(title="User 1 Task", description="Test Desc", priority="medium")
    created_task = create_task(task_data, user1.id, session)

    # Try to delete user1's task as user2
    success = delete_task(created_task.id, user2.id, session)

    # Verify deletion failed
    assert success is False

    # Verify the original task still exists
    still_exists = get_task_by_id(created_task.id, user1.id, session)
    assert still_exists is not None
    assert still_exists.title == "User 1 Task"