from fastapi import APIRouter, Depends, HTTPException, status, Query
from sqlmodel import Session
from typing import List

from ..services.task_service import (
    create_task, get_tasks, get_task_by_id, update_task, delete_task
)
from ..schemas.task import TaskCreate, TaskUpdate, TaskResponse
from ..database.session import get_session
from ..models.user import User
from ..utils.auth import get_current_user
from ..models.task import Task

router = APIRouter(tags=["Tasks"])


@router.get("/tasks", response_model=List[TaskResponse])
def read_tasks(
    skip: int = 0,
    limit: int = 100,
    completed: bool = Query(None, description="Filter by completion status"),
    priority: str = Query(None, description="Filter by priority (low, medium, high)"),
    sort_by: str = Query("created_at", description="Sort by field (created_at, updated_at, title, priority, due_date, completed)"),
    sort_order: str = Query("desc", description="Sort order (asc, desc)"),
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_session)
):
    """
    Get all tasks for the current user with optional filtering, sorting, and pagination
    """
    return get_tasks(
        user_id=current_user.id,
        db=db,
        skip=skip,
        limit=limit,
        completed=completed,
        priority=priority,
        sort_by=sort_by,
        sort_order=sort_order
    )


@router.post("/tasks", response_model=TaskResponse, status_code=status.HTTP_201_CREATED)
def create_new_task(
    task: TaskCreate,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_session)
):
    """
    Create a new task for the current user
    """
    return create_task(task, current_user.id, db)


@router.get("/tasks/{task_id}", response_model=TaskResponse)
def read_task(
    task_id: int,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_session)
):
    """
    Get a specific task by ID if it belongs to the current user
    """
    task = get_task_by_id(task_id, current_user.id, db)
    if not task:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Task not found"
        )
    return task


@router.put("/tasks/{task_id}", response_model=TaskResponse)
def update_existing_task(
    task_id: int,
    task_update: TaskUpdate,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_session)
):
    """
    Update a task if it belongs to the current user
    """
    updated_task = update_task(task_id, task_update, current_user.id, db)
    if not updated_task:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Task not found or not owned by user"
        )
    return updated_task


@router.delete("/tasks/{task_id}", status_code=status.HTTP_204_NO_CONTENT)
def delete_existing_task(
    task_id: int,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_session)
):
    """
    Delete a task if it belongs to the current user
    """
    success = delete_task(task_id, current_user.id, db)
    if not success:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Task not found or not owned by user"
        )
    # For 204 No Content, return nothing
    return

