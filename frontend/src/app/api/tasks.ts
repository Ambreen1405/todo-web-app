import { apiClient } from './client';
import { Task } from '../types/task';

interface CreateTaskData {
  title: string;
  description?: string;
  priority?: 'low' | 'medium' | 'high';
  due_date?: string;
  recurring?: 'daily' | 'weekly' | 'monthly' | 'none';
  category?: 'work' | 'home' | 'other';
}

interface UpdateTaskData {
  title?: string;
  description?: string;
  completed?: boolean;
  priority?: 'low' | 'medium' | 'high';
  due_date?: string;
  recurring?: 'daily' | 'weekly' | 'monthly' | 'none';
  category?: 'work' | 'home' | 'other';
}

export const tasksApi = {
  /**
   * Get all tasks for the current user
   */
  async getAllTasks(token: string): Promise<{ success: boolean; tasks?: Task[]; error?: string }> {
    const response = await apiClient.get('/tasks', {
      'Authorization': `Bearer ${token}`
    });

    if (response.success) {
      return { success: true, tasks: response.data as Task[] };
    } else {
      return { success: false, error: response.error };
    }
  },

  /**
   * Create a new task
   */
  async createTask(
    taskData: CreateTaskData,
    token: string
  ): Promise<{ success: boolean; task?: Task; error?: string }> {
    const response = await apiClient.post('/tasks', taskData, {
      'Authorization': `Bearer ${token}`
    });

    if (response.success && response.data) {
      return { success: true, task: response.data as Task };
    } else {
      return { success: false, error: response.error };
    }
  },

  /**
   * Update an existing task
   */
  async updateTask(
    taskId: number,
    taskData: UpdateTaskData,
    token: string
  ): Promise<{ success: boolean; task?: Task; error?: string }> {
    const response = await apiClient.put(`/tasks/${taskId}`, taskData, {
      'Authorization': `Bearer ${token}`
    });

    if (response.success && response.data) {
      return { success: true, task: response.data as Task };
    } else {
      return { success: false, error: response.error };
    }
  },

  /**
   * Delete a task
   */
  async deleteTask(taskId: number, token: string): Promise<{ success: boolean; error?: string }> {
    const response = await apiClient.delete(`/tasks/${taskId}`, {
      'Authorization': `Bearer ${token}`
    });

    if (response.success) {
      return { success: true };
    } else {
      return { success: false, error: response.error };
    }
  },

  /**
   * Get a specific task by ID
   */
  async getTaskById(
    taskId: number,
    token: string
  ): Promise<{ success: boolean; task?: Task; error?: string }> {
    const response = await apiClient.get(`/tasks/${taskId}`, {
      'Authorization': `Bearer ${token}`
    });

    if (response.success && response.data) {
      return { success: true, task: response.data as Task };
    } else {
      return { success: false, error: response.error };
    }
  }
};