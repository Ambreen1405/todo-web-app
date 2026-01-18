'use client';

import React, { useState } from 'react';
import { apiClient } from '../../api/client';
import { useAuth } from '../../context/auth';
import { useLoading } from '../../context/loading';
import { Task } from '../../types/task';
import { ActivityItem, ActivityType } from '../../types/activity';

interface TaskFormData {
  title: string;
  description: string;
  priority: 'low' | 'medium' | 'high';
  due_date: string;
}

interface TaskFormProps {
  onTaskCreated: (newTask: Task) => void;
}

const TaskForm: React.FC<TaskFormProps> = ({ onTaskCreated }) => {
  const [formData, setFormData] = useState<TaskFormData>({
    title: '',
    description: '',
    priority: 'medium',
    due_date: '',
  });
  const [error, setError] = useState<string | null>(null);
  const [localLoading, setLocalLoading] = useState(false);
  const { token } = useAuth();
  const { showGlobalLoader, hideGlobalLoader } = useLoading();

  const handleChange = (e: React.ChangeEvent<HTMLInputElement | HTMLTextAreaElement | HTMLSelectElement>) => {
    const { name, value } = e.target;
    setFormData(prev => ({
      ...prev,
      [name]: value
    }));
  };

  const handleSubmit = async (e: React.FormEvent) => {
    e.preventDefault();
    setLocalLoading(true);
    setError(null);

    // Show global loader
    showGlobalLoader();

    // Additional validation
    if (!formData.title.trim()) {
      setError('Title is required');
      setLocalLoading(false);
      hideGlobalLoader();
      return;
    }

    if (formData.title.length > 255) {
      setError('Title must be 255 characters or less');
      setLocalLoading(false);
      hideGlobalLoader();
      return;
    }

    if (formData.description && formData.description.length > 1000) {
      setError('Description must be 1000 characters or less');
      setLocalLoading(false);
      hideGlobalLoader();
      return;
    }

    if (!token) {
      setError('You must be logged in to create a task');
      setLocalLoading(false);
      hideGlobalLoader();
      return;
    }

    try {
      const response = await apiClient.post('/tasks', formData, {
        'Authorization': `Bearer ${token}`
      });

      if (response.success && response.data) {
        const newTask = response.data as Task;
        // Reset form
        setFormData({
          title: '',
          description: '',
          priority: 'medium',
          due_date: '',
        });

        // Emit activity event
        const activity: ActivityItem = {
          id: `act-${Date.now()}-${Math.random().toString(36).substr(2, 9)}`,
          type: 'task_created' as ActivityType,
          taskId: newTask.id,
          taskTitle: newTask.title,
          userId: newTask.user_id,
          timestamp: newTask.created_at || new Date().toISOString(),
          message: `Task "${newTask.title}" was created`
        };

        window.dispatchEvent(new CustomEvent('task-activity', { detail: activity }));

        onTaskCreated(newTask); // Pass the created task with timestamp data
      } else {
        setError(response.error || 'Failed to create task');
      }
    } catch (err) {
      setError('An error occurred while creating the task');
      console.error('Error creating task:', err);
    } finally {
      setLocalLoading(false);
      hideGlobalLoader();
    }
  };

  return (
    <div className="bg-white shadow overflow-hidden sm:rounded-md max-w-md w-full">
      <div className="px-4 py-5 sm:p-6">
        <h3 className="text-lg leading-6 font-medium text-gray-900 mb-4">
          Create New Task
        </h3>

        {error && (
          <div className="rounded-md bg-red-50 p-4 mb-4">
            <div className="text-sm text-red-700">{error}</div>
          </div>
        )}

        <form onSubmit={handleSubmit} className="space-y-4">
          <div>
            <label htmlFor="title" className="block text-sm font-medium text-gray-700">
              Title *
            </label>
            <input
              type="text"
              name="title"
              id="title"
              required
              value={formData.title}
              onChange={handleChange}
              className="mt-1 block w-full border border-gray-300 rounded-md shadow-sm py-2 px-3 focus:outline-none focus:ring-indigo-500 focus:border-indigo-500 sm:text-sm"
              placeholder="Task title"
            />
          </div>

          <div>
            <label htmlFor="description" className="block text-sm font-medium text-gray-700">
              Description
            </label>
            <textarea
              name="description"
              id="description"
              rows={3}
              value={formData.description}
              onChange={handleChange}
              className="mt-1 block w-full border border-gray-300 rounded-md shadow-sm py-2 px-3 focus:outline-none focus:ring-indigo-500 focus:border-indigo-500 sm:text-sm"
              placeholder="Task description"
            />
          </div>

          <div className="grid grid-cols-2 gap-4">
            <div>
              <label htmlFor="priority" className="block text-sm font-medium text-gray-700">
                Priority
              </label>
              <select
                name="priority"
                id="priority"
                value={formData.priority}
                onChange={handleChange}
                className="mt-1 block w-full bg-white border border-gray-300 rounded-md shadow-sm py-2 px-3 focus:outline-none focus:ring-indigo-500 focus:border-indigo-500 sm:text-sm"
              >
                <option value="low">Low</option>
                <option value="medium">Medium</option>
                <option value="high">High</option>
              </select>
            </div>

            <div>
              <label htmlFor="due_date" className="block text-sm font-medium text-gray-700">
                Due Date
              </label>
              <input
                type="date"
                name="due_date"
                id="due_date"
                value={formData.due_date}
                onChange={handleChange}
                className="mt-1 block w-full border border-gray-300 rounded-md shadow-sm py-2 px-3 focus:outline-none focus:ring-indigo-500 focus:border-indigo-500 sm:text-sm"
              />
            </div>
          </div>

          <div>
            <button
              type="submit"
              disabled={localLoading}
              className="w-full inline-flex justify-center py-2 px-4 border border-transparent shadow-sm text-sm font-medium rounded-md text-white bg-indigo-600 hover:bg-indigo-700 focus:outline-none focus:ring-2 focus:ring-offset-2 focus:ring-indigo-500 disabled:opacity-50"
            >
              {localLoading ? (
                <>
                  <svg className="animate-spin -ml-1 mr-2 h-4 w-4 text-white" xmlns="http://www.w3.org/2000/svg" fill="none" viewBox="0 0 24 24">
                    <circle className="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" strokeWidth="4"></circle>
                    <path className="opacity-75" fill="currentColor" d="M4 12a8 8 0 018-8V0C5.373 0 0 5.373 0 12h4zm2 5.291A7.962 7.962 0 014 12H0c0 3.042 1.135 5.824 3 7.938l3-2.647z"></path>
                  </svg>
                  Creating...
                </>
              ) : 'Create Task'}
            </button>
          </div>
        </form>
      </div>
    </div>
  );
};

export default TaskForm;