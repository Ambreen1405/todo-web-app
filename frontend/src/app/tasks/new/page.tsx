'use client';

import React, { useState, useEffect } from 'react';
import { useRouter } from 'next/navigation';
import { useAuth } from '../../context/auth';
import Link from 'next/link';
import { tasksApi } from '../../api/tasks';
import { CreateTaskData } from '../../api/tasks';
import { ActivityItem, ActivityType } from '../../types/activity';

const NewTaskPage: React.FC = () => {
  const router = useRouter();
  const { isAuthenticated, token } = useAuth();
  const [formData, setFormData] = useState<CreateTaskData>({
    title: '',
    description: '',
    priority: 'medium',
    recurring: 'none',
    category: 'other',
    due_date: ''
  });
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState('');

  const handleChange = (e: React.ChangeEvent<HTMLInputElement | HTMLTextAreaElement | HTMLSelectElement>) => {
    const { name, value } = e.target;
    setFormData(prev => ({
      ...prev,
      [name]: value
    }));
  };

  useEffect(() => {
    // Check authentication status
    if (!isAuthenticated || !token) {
      router.push('/login');
    }
  }, [isAuthenticated, token, router]);

  const handleSubmit = async (e: React.FormEvent) => {
    e.preventDefault();
    setError('');

    if (!formData.title.trim()) {
      setError('Title is required');
      return;
    }

    setLoading(true);

    try {
      // Capitalize the first letter of the title before creating the task
      const capitalizedFormData = {
        ...formData,
        title: formData.title.charAt(0).toUpperCase() + formData.title.slice(1)
      };

      // Actually create the task using the API
      const result = await tasksApi.createTask(
        capitalizedFormData,
        token!
      );

      if (result.success && result.task) {
        // Trigger event to update dashboard stats
        window.dispatchEvent(new CustomEvent('task-updated'));

        // Emit activity event for task creation
        const newTask = result.task;
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

        // Navigate to tasks page
        router.push('/tasks');
      } else {
        setError(result.error || 'Failed to create task');
        setLoading(false);
      }
    } catch (err) {
      setError('An error occurred while creating the task');
      setLoading(false);
      console.error('Error creating task:', err);
    }
  };

  if (!isAuthenticated || !token) {
    return null; // Will be redirected by useEffect
  }

  return (
    <div className="min-h-screen bg-gradient-to-br from-teal-400 via-yellow-200 to-teal-600 p-4">
      <div className="max-w-2xl mx-auto">
        {/* Header */}
        <header className="bg-white rounded-2xl shadow-xl p-6 mb-6">
          <div className="flex justify-between items-center">
            <h1 className="text-2xl font-bold text-teal-500">Create New Task</h1>
            <Link
              href="/tasks"
              className="text-gray-600 hover:text-gray-800 font-medium"
            >
              ← Back to Tasks
            </Link>
          </div>
        </header>

        {/* Task Form */}
        <main className="bg-white rounded-2xl shadow-xl p-6">
          <form onSubmit={handleSubmit} className="space-y-6">
            {error && (
              <div className="bg-red-50 border border-red-200 rounded-lg p-3">
                <p className="text-red-600 text-sm text-center">{error}</p>
              </div>
            )}

            <div>
              <label htmlFor="title" className="block text-sm font-medium text-gray-700 mb-2">
                Task Title 
              </label>
              <input
                id="title"
                name="title"
                type="text"
                value={formData.title}
                onChange={handleChange}
                className="w-full px-4 py-3 border border-teal-300 text-gray-600 rounded-lg focus:ring-2 focus:ring-teal-400 focus:border-transparent outline-none transition-all"
                placeholder="Enter task title"
              />
            </div>

            <div>
              <label htmlFor="description" className="block text-sm font-medium text-gray-700 mb-2">
                Description
              </label>
              <textarea
                id="description"
                name="description"
                value={formData.description}
                onChange={handleChange}
                rows={4}
                className="w-full px-4 py-3 border border-teal-300 text-gray-600 rounded-lg focus:ring-2 focus:ring-teal-400 focus:border-transparent outline-none transition-all resize-none"
                placeholder="Enter task description (optional)"
              />
            </div>

            <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
              <div>
                <label htmlFor="priority" className="block text-sm font-medium text-gray-700 mb-2">
                  Priority
                </label>
                <select
                  id="priority"
                  name="priority"
                  value={formData.priority}
                  onChange={handleChange}
                  className="w-full px-4 py-3 border border-teal-300 text-gray-600 rounded-lg focus:ring-2 focus:ring-teal-400 focus:border-transparent outline-none transition-all"
                >
                  <option value="low">Low</option>
                  <option value="medium">Medium</option>
                  <option value="high">High</option>
                </select>
              </div>

              <div>
                <label htmlFor="category" className="block text-sm font-medium text-gray-700 mb-2">
                  Category
                </label>
                <select
                  id="category"
                  name="category"
                  value={formData.category}
                  onChange={handleChange}
                  className="w-full px-4 py-3 border border-teal-300 text-gray-600 rounded-lg focus:ring-2 focus:ring-teal-400 focus:border-transparent outline-none transition-all"
                >
                  <option value="work">Work</option>
                  <option value="home">Home</option>
                  <option value="other">Other</option>
                </select>
              </div>
            </div>

            <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
              <div>
                <label htmlFor="due_date" className="block text-sm font-medium text-gray-700 mb-2">
                  Due Date (optional)
                </label>
                <input
                  id="due_date"
                  name="due_date"
                  type="date"
                  value={formData.due_date}
                  onChange={handleChange}
                  className="w-full px-4 py-3 border border-teal-300 text-gray-600 rounded-lg focus:ring-2 focus:ring-teal-400 focus:border-transparent outline-none transition-all"
                />
              </div>

              <div>
                <label htmlFor="recurring" className="block text-sm font-medium text-gray-700 mb-2">
                  Recurring:
                </label>
                <select
                  id="recurring"
                  name="recurring"
                  value={formData.recurring}
                  onChange={handleChange}
                  className="w-full px-4 py-3 border border-teal-300 text-gray-600 rounded-lg focus:ring-2 focus:ring-teal-400 focus:border-transparent outline-none transition-all"
                >
                  <option value="none">None</option>
                  <option value="daily">Daily</option>
                  <option value="weekly">Weekly</option>
                  <option value="monthly">Monthly</option>
                </select>
              </div>
            </div>

            <div className="flex space-x-4 pt-4">
              <button
                type="submit"
                disabled={loading}
                className="flex-1 bg-gradient-to-br from-teal-400 via-yellow-200 to-teal-300 hover:from-teal-500 hover:via-yellow-300 hover:to-teal-400 text-black py-3 rounded-lg font-semibold focus:outline-none focus:ring-2 focus:ring-teal-500 focus:ring-offset-2 transition-all disabled:opacity-50"
              >
                {loading ? (
                  <div className="flex items-center justify-center">
                    <svg className="animate-spin -ml-1 mr-3 h-5 w-5 text-white" xmlns="http://www.w3.org/2000/svg" fill="none" viewBox="0 0 24 24">
                      <circle className="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" strokeWidth="4"></circle>
                      <path className="opacity-75" fill="currentColor" d="M4 12a8 8 0 018-8V0C5.373 0 0 5.373 0 12h4zm2 5.291A7.962 7.962 0 014 12H0c0 3.042 1.135 5.824 3 7.938l3-2.647z"></path>
                    </svg>
                    Creating...
                  </div>
                ) : (
                  'Create Task'
                )}
              </button>

              <Link
                href="/tasks"
                className="flex-1 border text-gray-700 bg-gray-200 py-3 rounded-lg font-semibold text-center hover:bg-gray-300 focus:outline-none focus:ring-2 focus:ring-gray-500 focus:ring-offset-2 transition-all"
              >
                Cancel
              </Link>
            </div>
          </form>
        </main>
      </div>
    </div>
  );
};

export default NewTaskPage;