'use client';

import React, { useState, useEffect } from 'react';
import { apiClient } from '../../api/client';
import { Task } from '../../types/task';
import { useAuth } from '../../context/auth';
import { useLoading } from '../../context/loading';
import { ActivityItem, ActivityType } from '../../types/activity';
import { useTaskDelete } from '../../../context/task-delete-context';

interface TaskItemProps {
  task: Task;
  onTaskUpdate: () => void;
  onTaskDeleted?: (taskId: number) => void;
}

const TaskItem: React.FC<TaskItemProps> = ({ task, onTaskUpdate, onTaskDeleted }) => {
  const [isEditing, setIsEditing] = useState(false);
  const [editedTitle, setEditedTitle] = useState(task.title);
  const [editedDescription, setEditedDescription] = useState(task.description || '');
  const [editedDueDate, setEditedDueDate] = useState(task.due_date || '');
  const [editError, setEditError] = useState<string | null>(null);
  const [localLoading, setLocalLoading] = useState(false);
  const { confirmingTaskId, setConfirmingTaskId } = useTaskDelete();
  const showConfirmDelete = confirmingTaskId === task.id;

  const { token } = useAuth();
  const { showGlobalLoader, hideGlobalLoader } = useLoading();


  const handleToggleComplete = async () => {
    if (!token) return;

    setLocalLoading(true);
    showGlobalLoader();
    try {
      const response = await apiClient.put(`/tasks/${task.id}`, {
        completed: !task.completed
      }, {
        'Authorization': `Bearer ${token}`
      });

      if (response.success) {
        // Emit activity event for completion/uncompletion
        const activityType: ActivityType = !task.completed ? 'task_completed' : 'task_uncompleted';
        const activity: ActivityItem = {
          id: `act-${Date.now()}-${Math.random().toString(36).substr(2, 9)}`,
          type: activityType,
          taskId: task.id,
          taskTitle: task.title,
          userId: task.user_id,
          timestamp: new Date().toISOString(),
          message: !task.completed
            ? `Task "${task.title}" was marked as completed`
            : `Task "${task.title}" was marked as incomplete`
        };

        window.dispatchEvent(new CustomEvent('task-activity', { detail: activity }));

        onTaskUpdate(); // Refresh the task list
      } else {
        console.error('Failed to update task:', response.error);
      }
    } catch (err) {
      console.error('Error updating task:', err);
    } finally {
      setLocalLoading(false);
      hideGlobalLoader();
    }
  };

  const handleDelete = async () => {
    setConfirmingTaskId(task.id);
  };

  const handleConfirmDelete = async () => {
    if (!token) return;

    setLocalLoading(true);
    showGlobalLoader();
    try {
      const response = await apiClient.delete(`/tasks/${task.id}`, {
        'Authorization': `Bearer ${token}`
      });

      if (response.success) {
        // Emit activity event for deletion
        const activity: ActivityItem = {
          id: `act-${Date.now()}-${Math.random().toString(36).substr(2, 9)}`,
          type: 'task_deleted',
          taskId: task.id,
          taskTitle: task.title,
          userId: task.user_id,
          timestamp: new Date().toISOString(),
          message: `Task "${task.title}" was deleted`
        };

        window.dispatchEvent(new CustomEvent('task-activity', { detail: activity }));

        // Optimistically update the UI by removing the task immediately
        if (onTaskDeleted) {
          onTaskDeleted(task.id);
        } else {
          // Fallback to the original behavior
          onTaskUpdate();
        }
      } else {
        console.error('Failed to delete task:', response.error);
      }
    } catch (err) {
      console.error('Error deleting task:', err);
    } finally {
      setLocalLoading(false);
      hideGlobalLoader();
      setConfirmingTaskId(null);
    }
  };

  const handleCancelDelete = () => {
    setConfirmingTaskId(null);
  };

  const handleSaveEdit = async () => {
    if (!token) return;

    // Clear previous errors
    setEditError(null);

    // Additional validation
    if (!editedTitle.trim()) {
      setEditError('Title is required');
      return;
    }

    if (editedTitle.length > 255) {
      setEditError('Title must be 255 characters or less');
      return;
    }

    if (editedDescription && editedDescription.length > 1000) {
      setEditError('Description must be 1000 characters or less');
      return;
    }

    setLocalLoading(true);
    showGlobalLoader();
    try {
      // Capitalize the first letter of the title before sending to API
      const capitalizedTitle = editedTitle.charAt(0).toUpperCase() + editedTitle.slice(1);

      const response = await apiClient.put(`/tasks/${task.id}`, {
        title: capitalizedTitle,
        description: editedDescription,
        due_date: editedDueDate || null,
        completed: task.completed
      }, {
        'Authorization': `Bearer ${token}`
      });

      if (response.success) {
        // Emit activity event for update
        const activity: ActivityItem = {
          id: `act-${Date.now()}-${Math.random().toString(36).substr(2, 9)}`,
          type: 'task_updated',
          taskId: task.id,
          taskTitle: editedTitle,
          userId: task.user_id,
          timestamp: new Date().toISOString(),
          message: `Task "${editedTitle}" was updated`
        };

        window.dispatchEvent(new CustomEvent('task-activity', { detail: activity }));

        setIsEditing(false);
        onTaskUpdate(); // Refresh the task list
      } else {
        setEditError(response.error || 'Failed to update task');
        console.error('Failed to update task:', response.error);
      }
    } catch (err) {
      setEditError('An error occurred while updating the task');
      console.error('Error updating task:', err);
    } finally {
      setLocalLoading(false);
      hideGlobalLoader();
    }
  };

  const formatDate = (dateString: string) => {
    return new Date(dateString).toLocaleDateString();
  };

  const formatDateTime = (dateString: string) => {
    return new Date(dateString).toLocaleString();
  };

  return (
    <>
      <li className={`py-4 ${localLoading ? 'opacity-50' : ''}`}>
        <div className="flex items-start">
          <div className="flex items-center h-5">
            <input
              type="checkbox"
              checked={task.completed}
              onChange={handleToggleComplete}
              className="focus:ring-indigo-500 h-4 w-4 text-indigo-600 border-gray-300 rounded"
              disabled={localLoading}
            />
          </div>
          <div className="ml-3 min-w-0 flex-1">
            {isEditing ? (
              <div className="space-y-3">
                <input
                  type="text"
                  value={editedTitle}
                  onChange={(e) => setEditedTitle(e.target.value)}
                  className="block w-full px-3 py-2 border border-gray-300 rounded-md shadow-sm focus:outline-none focus:ring-indigo-500 focus:border-indigo-500 sm:text-sm"
                  placeholder="Task title"
                />
                <textarea
                  value={editedDescription}
                  onChange={(e) => setEditedDescription(e.target.value)}
                  className="block w-full px-3 py-2 border border-gray-300 rounded-md shadow-sm focus:outline-none focus:ring-indigo-500 focus:border-indigo-500 sm:text-sm"
                  rows={3}
                  placeholder="Task description"
                />
                <div>
                  <label htmlFor="edit-due-date" className="block text-xs font-medium text-gray-700 mb-1">
                    Due Date
                  </label>
                  <input
                    type="date"
                    id="edit-due-date"
                    value={editedDueDate}
                    onChange={(e) => setEditedDueDate(e.target.value)}
                    className="block w-full px-3 py-2 border border-gray-300 rounded-md shadow-sm focus:outline-none focus:ring-indigo-500 focus:border-indigo-500 sm:text-sm"
                  />
                </div>

                {editError && (
                  <div className="rounded-md bg-red-50 p-2">
                    <div className="text-sm text-red-700">{editError}</div>
                  </div>
                )}

                <div className="flex space-x-2">
                  <button
                    onClick={handleSaveEdit}
                    disabled={localLoading}
                    className="inline-flex items-center px-3 py-1 border border-transparent text-sm font-medium rounded-md shadow-sm text-white bg-green-600 hover:bg-green-700 focus:outline-none focus:ring-2 focus:ring-offset-2 focus:ring-green-500 disabled:opacity-50"
                  >
                    Save
                  </button>
                  <button
                    onClick={() => {
                      setIsEditing(false);
                      setEditError(null); // Clear error when canceling
                    }}
                    className="inline-flex items-center px-3 py-1 border border-gray-300 text-sm font-medium rounded-md shadow-sm text-gray-700 bg-white hover:bg-gray-50 focus:outline-none focus:ring-2 focus:ring-offset-2 focus:ring-indigo-500"
                  >
                    Cancel
                  </button>
                </div>
              </div>
            ) : (
              <div>
                <p className={`text-sm font-medium text-gray-900 ${task.completed ? 'line-through text-gray-500' : ''}`}>
                  {task.title}
                </p>
                {task.description && (
                  <p className={`text-sm text-gray-500 mt-1 ${task.completed ? 'line-through' : ''}`}>
                    {task.description}
                  </p>
                )}
                {task.due_date && (
                  <p className="text-xs text-gray-400 mt-1">
                    Due: {formatDate(task.due_date)}
                  </p>
                )}
                <div className="flex flex-wrap gap-1 mt-1">
                  <p className="text-xs text-gray-400">
                    Priority: <span className={`px-2 inline-flex text-xs leading-5 font-semibold rounded-full ${
                      task.priority === 'high' ? 'bg-red-100 text-red-800' :
                      task.priority === 'medium' ? 'bg-yellow-100 text-yellow-800' :
                      'bg-green-100 text-green-800'
                    }`}>
                      {task.priority}
                    </span>
                  </p>
                  {task.category && (
                    <p className="text-xs text-gray-400">
                      Category: <span className={`px-2 inline-flex text-xs leading-5 font-semibold rounded-full ${
                        task.category === 'work' ? 'bg-blue-100 text-blue-800' :
                        task.category === 'home' ? 'bg-purple-100 text-purple-800' :
                        'bg-gray-100 text-gray-800'
                      }`}>
                        {task.category.charAt(0).toUpperCase() + task.category.slice(1)}
                      </span>
                    </p>
                  )}
                </div>
                <p className="text-xs text-gray-400 mt-1">
                  Created: {task.created_at ? formatDateTime(task.created_at) : 'Just now'}
                </p>
              </div>
            )}
          </div>
          <div className="ml-4 flex-shrink-0 flex flex-col sm:flex-row space-x-0 sm:space-x-2 space-y-2 sm:space-y-0">
            {!isEditing && (
              <>
                <button
                  onClick={() => setIsEditing(true)}
                  className="inline-flex items-center px-2 py-1 sm:px-2.5 sm:py-0.5 border border-gray-300 text-xs font-medium rounded text-gray-700 bg-white hover:bg-gray-50 focus:outline-none focus:ring-2 focus:ring-offset-2 focus:ring-indigo-500"
                >
                  Edit
                </button>
                {showConfirmDelete ? (
                  <div className="flex space-x-2">
                    <button
                      onClick={handleConfirmDelete}
                      disabled={localLoading}
                      className="inline-flex items-center px-2 py-1 sm:px-2.5 sm:py-0.5 border border-transparent text-xs font-medium rounded text-white bg-red-600 hover:bg-red-700 focus:outline-none focus:ring-2 focus:ring-offset-2 focus:ring-red-500 disabled:opacity-50"
                    >
                      {localLoading ? 'Deleting...' : 'Confirm Delete'}
                    </button>
                    <button
                      onClick={handleCancelDelete}
                      disabled={localLoading}
                      className="inline-flex items-center px-2 py-1 sm:px-2.5 sm:py-0.5 border border-gray-300 text-xs font-medium rounded text-gray-700 bg-white hover:bg-gray-50 focus:outline-none focus:ring-2 focus:ring-offset-2 focus:ring-indigo-500 disabled:opacity-50"
                    >
                      Cancel
                    </button>
                  </div>
                ) : (
                  <button
                    onClick={handleDelete}
                    className="inline-flex items-center px-2 py-1 sm:px-2.5 sm:py-0.5 border border-transparent text-xs font-medium rounded text-white bg-red-600 hover:bg-red-700 focus:outline-none focus:ring-2 focus:ring-offset-2 focus:ring-red-500"
                  >
                    Delete
                  </button>
                )}
              </>
            )}
          </div>
        </div>
      </li>

    </>
  );
};

export default TaskItem;