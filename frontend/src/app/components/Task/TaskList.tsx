'use client';

import React, { useState, useEffect, useCallback } from 'react';
import TaskItem from './TaskItem';
import TaskForm from './TaskForm';
import { apiClient } from '../../api/client';
import { Task } from '../../types/task';
import { useAuth } from '../../context/auth';

interface TaskListProps {
  onTaskUpdate?: () => void;
}

const TaskList: React.FC<TaskListProps> = ({ onTaskUpdate }) => {
  const [tasks, setTasks] = useState<Task[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [filterCompleted, setFilterCompleted] = useState<'all' | 'completed' | 'pending'>('all');
  const [filterPriority, setFilterPriority] = useState<'all' | 'low' | 'medium' | 'high'>('all');

  const { token } = useAuth();

  useEffect(() => {
    fetchTasks();
  }, [filterCompleted, filterPriority]); // Fetch tasks when filters change

  const fetchTasks = async () => {
    if (!token) return;

    setLoading(true);
    setError(null);

    // Build query parameters based on filters
    const queryParams = new URLSearchParams();
    if (filterCompleted !== 'all') {
      queryParams.append('completed', filterCompleted === 'completed' ? 'true' : 'false');
    }
    if (filterPriority !== 'all') {
      queryParams.append('priority', filterPriority);
    }

    const queryString = queryParams.toString();
    const endpoint = queryString ? `/tasks?${queryString}` : '/tasks';

    try {
      const response = await apiClient.get(endpoint, {
        'Authorization': `Bearer ${token}`
      });

      if (response.success) {
        setTasks(response.data as Task[]);
      } else {
        setError(response.error || 'Failed to fetch tasks');
      }
    } catch (err) {
      setError('An error occurred while fetching tasks');
      console.error('Error fetching tasks:', err);
    } finally {
      setLoading(false);
    }
  };

  // Optimistic updates for task operations
  const addTaskOptimistically = useCallback((newTask: Task) => {
    setTasks(prev => [newTask, ...prev]); // Add to the beginning of the list
  }, []);

  const updateTaskOptimistically = useCallback((updatedTask: Task) => {
    setTasks(prev => prev.map(task => task.id === updatedTask.id ? updatedTask : task));
  }, []);

  const deleteTaskOptimistically = useCallback((taskId: number) => {
    setTasks(prev => prev.filter(task => task.id !== taskId));
  }, []);

  const handleTaskCreated = useCallback((newTask: Task) => {
    // Optimistically add the task before refetching
    addTaskOptimistically(newTask);
  }, [addTaskOptimistically]);

  const handleTaskDeleted = useCallback((taskId: number) => {
    // Optimistically delete the task from the UI
    deleteTaskOptimistically(taskId);

    // Call the parent's onTaskUpdate if provided
    if (onTaskUpdate) {
      onTaskUpdate();
    }
  }, [deleteTaskOptimistically, onTaskUpdate]);

  // Expose optimistic update functions to child components
  const optimisticUpdates = {
    addTask: addTaskOptimistically,
    updateTask: updateTaskOptimistically,
    deleteTask: deleteTaskOptimistically,
    refetchTasks: fetchTasks
  };

  if (loading) {
    return (
      <div className="space-y-4">
        {/* Skeleton loader for tasks */}
        {[...Array(3)].map((_, index) => (
          <div key={index} className="animate-pulse bg-white p-4 rounded-md shadow">
            <div className="h-4 bg-gray-200 rounded w-3/4 mb-2"></div>
            <div className="h-3 bg-gray-200 rounded w-1/2 mb-2"></div>
            <div className="h-3 bg-gray-200 rounded w-1/4"></div>
          </div>
        ))}
      </div>
    );
  }

  if (error) {
    return (
      <div className="rounded-md bg-red-50 p-4 mb-4">
        <div className="text-sm text-red-700">{error}</div>
      </div>
    );
  }

  return (
    <div className="mt-8">
      <div className="flex justify-between items-center mb-6">
        <h2 className="text-2xl font-bold text-gray-900">Your Tasks</h2>
        <TaskForm onTaskCreated={handleTaskCreated} />
      </div>

      {/* Filtering controls */}
      <div className="flex flex-wrap gap-4 mb-6 p-4 bg-gray-50 rounded-lg">
        <div>
          <label htmlFor="filter-completed" className="block text-sm font-medium text-gray-700 mb-1">
            Status
          </label>
          <select
            id="filter-completed"
            value={filterCompleted}
            onChange={(e) => setFilterCompleted(e.target.value as 'all' | 'completed' | 'pending')}
            className="block w-full pl-3 pr-10 py-2 text-base border-gray-300 focus:outline-none focus:ring-indigo-500 focus:border-indigo-500 sm:text-sm rounded-md"
          >
            <option value="all">All Tasks</option>
            <option value="pending">Pending</option>
            <option value="completed">Completed</option>
          </select>
        </div>

        <div>
          <label htmlFor="filter-priority" className="block text-sm font-medium text-gray-700 mb-1">
            Priority
          </label>
          <select
            id="filter-priority"
            value={filterPriority}
            onChange={(e) => setFilterPriority(e.target.value as 'all' | 'low' | 'medium' | 'high')}
            className="block w-full pl-3 pr-10 py-2 text-base border-gray-300 focus:outline-none focus:ring-indigo-500 focus:border-indigo-500 sm:text-sm rounded-md"
          >
            <option value="all">All Priorities</option>
            <option value="low">Low</option>
            <option value="medium">Medium</option>
            <option value="high">High</option>
          </select>
        </div>
      </div>

      {tasks.length === 0 ? (
        <div className="text-center py-8">
          <p className="text-gray-500">No tasks yet. Create your first task!</p>
        </div>
      ) : (
        <ul className="divide-y divide-gray-200">
          {tasks.map((task) => (
            <TaskItem
              key={task.id}
              task={task}
              onTaskUpdate={fetchTasks} // For other updates (edit, toggle completion, etc.)
              onTaskDeleted={handleTaskDeleted} // Only for optimistic deletion
            />
          ))}
        </ul>
      )}
    </div>
  );
};

export default TaskList;