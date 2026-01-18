from sqlmodel import Session, select
from fastapi import HTTPException, status
from typing import List, Optional

from ..models.task import Task, TaskCreate, TaskUpdate
from ..models.user import User
from ..models.activity_log import ActivityActionEnum
from ..utils.logging import logger
from ..utils.activity_logger import log_activity


def create_task(task_data: TaskCreate, user_id: int, db: Session) -> Task:
    """
    Create a new task for the specified user
    """
    try:
        # Validate task data
        if not task_data.title or len(task_data.title.strip()) == 0:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Task title is required"
            )

        if len(task_data.title) > 255:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Task title must be 255 characters or less"
            )

        if task_data.priority and task_data.priority not in ["low", "medium", "high"]:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Priority must be one of: low, medium, high"
            )

        # Create task with the provided user_id
        db_task = Task(
            **task_data.dict(),
            user_id=user_id
        )

        db.add(db_task)
        db.commit()
        db.refresh(db_task)

        # Log the activity
        log_activity(
            db=db,
            user_id=user_id,
            action=ActivityActionEnum.TASK_CREATED,
            task_id=db_task.id,
            task_title=db_task.title
        )

        logger.info(f"Task created with ID: {db_task.id} for user ID: {user_id}")
        return db_task
    except HTTPException:
        # Re-raise HTTP exceptions
        raise
    except Exception as e:
        logger.error(f"Unexpected error during task creation: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="An unexpected error occurred during task creation"
        )


def get_tasks(
    user_id: int,
    db: Session,
    skip: int = 0,
    limit: int = 100,
    completed: Optional[bool] = None,
    priority: Optional[str] = None,
    sort_by: str = "created_at",
    sort_order: str = "desc"
) -> List[Task]:
    """
    Get all tasks for the specified user with optional filtering, sorting, and pagination
    """
    query = select(Task).where(Task.user_id == user_id)

    # Apply filters
    if completed is not None:
        query = query.where(Task.completed == completed)

    if priority is not None:
        query = query.where(Task.priority == priority)

    # Apply sorting
    if sort_by == "created_at":
        if sort_order == "asc":
            query = query.order_by(Task.created_at.asc())
        else:
            query = query.order_by(Task.created_at.desc())
    elif sort_by == "updated_at":
        if sort_order == "asc":
            query = query.order_by(Task.updated_at.asc())
        else:
            query = query.order_by(Task.updated_at.desc())
    elif sort_by == "title":
        if sort_order == "asc":
            query = query.order_by(Task.title.asc())
        else:
            query = query.order_by(Task.title.desc())
    elif sort_by == "priority":
        if sort_order == "asc":
            query = query.order_by(Task.priority.asc())
        else:
            query = query.order_by(Task.priority.desc())
    elif sort_by == "due_date":
        if sort_order == "asc":
            query = query.order_by(Task.due_date.asc())
        else:
            query = query.order_by(Task.due_date.desc())
    elif sort_by == "completed":
        if sort_order == "asc":
            query = query.order_by(Task.completed.asc())
        else:
            query = query.order_by(Task.completed.desc())

    # Apply pagination
    query_with_pagination = query.offset(skip).limit(limit)
    tasks = db.exec(query_with_pagination).all()

    logger.info(f"Retrieved {len(tasks)} tasks for user ID: {user_id}")
    return tasks


def get_task_by_id(task_id: int, user_id: int, db: Session) -> Optional[Task]:
    """
    Get a specific task by ID if it belongs to the specified user
    """
    try:
        task = db.exec(
            select(Task)
            .where(Task.id == task_id)
            .where(Task.user_id == user_id)
        ).first()

        if task:
            logger.info(f"Retrieved task ID: {task_id} for user ID: {user_id}")
        else:
            logger.info(f"Task ID: {task_id} not found for user ID: {user_id}")

        return task
    except Exception as e:
        logger.error(f"Unexpected error during task retrieval: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="An unexpected error occurred during task retrieval"
        )


def update_task(task_id: int, task_data: TaskUpdate, user_id: int, db: Session) -> Optional[Task]:
    """
    Update a task if it belongs to the specified user
    """
    try:
        db_task = get_task_by_id(task_id, user_id, db)

        if not db_task:
            logger.warning(f"Attempt to update non-existent task ID: {task_id} for user ID: {user_id}")
            return None

        # Validate update data if provided
        if task_data.title and len(task_data.title) > 255:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Task title must be 255 characters or less"
            )

        if task_data.priority and task_data.priority not in ["low", "medium", "high"]:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Priority must be one of: low, medium, high"
            )

        # Store original values for activity logging
        original_completed = db_task.completed
        original_title = db_task.title

        # Update only the fields that are provided
        update_data = task_data.dict(exclude_unset=True)
        for field, value in update_data.items():
            setattr(db_task, field, value)

        # Update the updated_at timestamp
        from datetime import datetime
        db_task.updated_at = datetime.utcnow()

        db.add(db_task)
        db.commit()
        db.refresh(db_task)

        # Determine the type of update and log the activity
        action = ActivityActionEnum.TASK_UPDATED

        # Check if completed field was provided in the update by checking if it's in the dict with exclude_unset=True
        update_data = task_data.dict(exclude_unset=True)
        if 'completed' in update_data and task_data.completed != original_completed:
            action = ActivityActionEnum.TASK_COMPLETED if task_data.completed else ActivityActionEnum.TASK_UNCOMPLETED

        log_activity(
            db=db,
            user_id=user_id,
            action=action,
            task_id=db_task.id,
            task_title=original_title
        )

        logger.info(f"Updated task ID: {task_id} for user ID: {user_id}")
        return db_task
    except HTTPException:
        # Re-raise HTTP exceptions
        raise
    except Exception as e:
        logger.error(f"Unexpected error during task update: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="An unexpected error occurred during task update"
        )


def delete_task(task_id: int, user_id: int, db: Session) -> bool:
    """
    Delete a task if it belongs to the specified user
    """
    try:
        db_task = get_task_by_id(task_id, user_id, db)

        if not db_task:
            logger.warning(f"Attempt to delete non-existent task ID: {task_id} for user ID: {user_id}")
            return False

        # Store task details before deletion for activity logging
        task_title = db_task.title
        task_id_to_log = db_task.id

        db.delete(db_task)
        db.commit()

        # Log the activity
        log_activity(
            db=db,
            user_id=user_id,
            action=ActivityActionEnum.TASK_DELETED,
            task_id=task_id_to_log,
            task_title=task_title
        )

        logger.info(f"Deleted task ID: {task_id} for user ID: {user_id}")
        return True
    except Exception as e:
        logger.error(f"Unexpected error during task deletion: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="An unexpected error occurred during task deletion"
        )