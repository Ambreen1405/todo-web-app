'use client';

import React, { useState, useEffect, useMemo } from 'react';
import { useRouter } from 'next/navigation';
import { useAuth } from '../context/auth';
import Link from 'next/link';
import { tasksApi, CreateTaskData } from '../api/tasks';
import { Task } from '../types/task';
import { ActivityType, ActivityItem } from '../types/activity';

const TasksPage: React.FC = () => {
  const router = useRouter();
  const { isAuthenticated, token } = useAuth();
  const [tasks, setTasks] = useState<Task[]>([]);
  const [filteredTasks, setFilteredTasks] = useState<Task[]>([]);
  const [loading, setLoading] = useState(true);
  const [searchTerm, setSearchTerm] = useState('');
  const [filter, setFilter] = useState<'all' | 'active' | 'completed' | 'overdue' | 'upcoming'>('all');
  const [sortBy, setSortBy] = useState<'created_at' | 'due_date' | 'priority' | 'title'>('created_at');
  const [sortOrder, setSortOrder] = useState<'asc' | 'desc'>('desc');

  // Helper to get "Today" at midnight for accurate comparisons
  const getToday = () => {
    const d = new Date();
    d.setHours(0, 0, 0, 0);
    return d;
  };

  const applyFiltersAndSorting = (tasksToFilter: Task[]) => {
    let result = [...tasksToFilter];
    const today = getToday();

    // 1. Search Filter
    if (searchTerm) {
      const term = searchTerm.toLowerCase();
      result = result.filter(task =>
        task.title.toLowerCase().includes(term) ||
        task.description?.toLowerCase().includes(term)
      );
    }

    // 2. Status Filter
    result = result.filter(task => {
      const taskDate = task.due_date ? new Date(task.due_date) : null;
      if (taskDate) taskDate.setHours(0,0,0,0);

      switch (filter) {
        case 'active': return !task.completed;
        case 'completed': return task.completed;
        case 'overdue': 
            return !task.completed && taskDate && taskDate < today;
        case 'upcoming': 
            return !task.completed && taskDate && taskDate >= today;
        default: return true;
      }
    });

    // 3. Sorting
    result.sort((a, b) => {
      let comparison = 0;
      switch (sortBy) {
        case 'title':
          comparison = a.title.localeCompare(b.title);
          break;
        case 'due_date':
          if (!a.due_date && !b.due_date) comparison = 0;
          else if (!a.due_date) comparison = 1;
          else if (!b.due_date) comparison = -1;
          else comparison = new Date(a.due_date).getTime() - new Date(b.due_date).getTime();
          break;
        case 'priority':
          const pOrder = { 'high': 3, 'medium': 2, 'low': 1 };
          comparison = pOrder[b.priority] - pOrder[a.priority];
          break;
        default:
          comparison = new Date(b.created_at).getTime() - new Date(a.created_at).getTime();
      }
      return sortOrder === 'asc' ? comparison : -comparison;
    });

    setFilteredTasks(result);
  };

  // --- API Handlers ---

  const loadTasks = async () => {
    if (!isAuthenticated || !token) {
      router.push('/login');
      return;
    }

    try {
      const response = await tasksApi.getAllTasks(token);
      if (response.success) {
        const loadedTasks = response.tasks || [];
        setTasks(loadedTasks);
        applyFiltersAndSorting(loadedTasks);
      }
    } catch (error) {
      console.error('Failed to load tasks', error);
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => { loadTasks(); }, [isAuthenticated, token]);

  useEffect(() => {
    applyFiltersAndSorting(tasks);
  }, [searchTerm, filter, sortBy, sortOrder, tasks]);

  const handleToggleComplete = async (task: Task) => {
    if (!token) return;
    try {
      const result = await tasksApi.updateTask(task.id, { completed: !task.completed }, token);
      if (result.success && result.task) {
        // Use the updated task from the API response
        const updatedTask = result.task;
        setTasks(prev => prev.map(t => t.id === task.id ? updatedTask : t));
        window.dispatchEvent(new CustomEvent('task-updated'));

        // Emit activity event for completion/uncompletion
        const activityType: ActivityType = !task.completed ? 'task_completed' : 'task_uncompleted';
        const activity: ActivityItem = {
          id: `act-${Date.now()}-${Math.random().toString(36).substr(2, 9)}`,
          type: activityType,
          taskId: updatedTask.id,
          taskTitle: updatedTask.title,
          userId: updatedTask.user_id,
          timestamp: new Date().toISOString(),
          message: !task.completed
            ? `Task "${updatedTask.title}" was marked as completed`
            : `Task "${updatedTask.title}" was marked as incomplete`
        };
        window.dispatchEvent(new CustomEvent('task-activity', { detail: activity }));
      }
    } catch (e) { alert("Failed to update task."); }
  };

  const handleDeleteTask = async (taskId: number) => {
    if (!token) return;
    try {
      const result = await tasksApi.deleteTask(taskId, token);
      if (result.success) {
        setTasks(prev => prev.filter(t => t.id !== taskId));
        window.dispatchEvent(new CustomEvent('task-updated'));

        // Find the task that was deleted to create activity event
        const deletedTask = tasks.find(t => t.id === taskId);
        if (deletedTask) {
          const activity: ActivityItem = {
            id: `act-${Date.now()}-${Math.random().toString(36).substr(2, 9)}`,
            type: 'task_deleted',
            taskId: deletedTask.id,
            taskTitle: deletedTask.title,
            userId: deletedTask.user_id,
            timestamp: new Date().toISOString(),
            message: `Task "${deletedTask.title}" was deleted`
          };
          window.dispatchEvent(new CustomEvent('task-activity', { detail: activity }));
        }
      }
    } catch (e) { alert("Could not delete task."); }
  };

  // --- Modal Logic ---
  const [editingTask, setEditingTask] = useState<Task | null>(null);
  const [editFormData, setEditFormData] = useState<CreateTaskData>({
    title: '', description: '', priority: 'medium', recurring: 'none', category: 'other', due_date: ''
  });

  const handleEditChange = (e: React.ChangeEvent<HTMLInputElement | HTMLTextAreaElement | HTMLSelectElement>) => {
    const { name, value } = e.target;
    setEditFormData(prev => ({
      ...prev,
      [name]: value
    }));
  };

  const handleEditClick = (task: Task) => {
    setEditingTask(task);
    setEditFormData({
      title: task.title,
      description: task.description || '',
      priority: task.priority,
      recurring: task.recurring || 'none',
      category: task.category || 'other',
      due_date: task.due_date ? new Date(task.due_date).toISOString().split('T')[0] : '' // Format for <input type="date">
    });
  };

  const handleSaveEdit = async () => {
    if (!token || !editingTask) return;
    try {
      // Format the due_date to ISO string if it exists and capitalize the title
      const formattedData = {
        ...editFormData,
        title: editFormData.title.charAt(0).toUpperCase() + editFormData.title.slice(1),
        due_date: editFormData.due_date ? new Date(editFormData.due_date + 'T00:00:00').toISOString() : undefined
      };

      const result = await tasksApi.updateTask(editingTask.id, formattedData, token);
      if (result.success && result.task) {
        // Update the task in the local state - use the response data
        const updatedTask = result.task;
        setTasks(prev => prev.map(t =>
          t.id === editingTask.id
            ? {
                ...updatedTask,
                due_date: updatedTask.due_date || undefined
              }
            : t
        ));
        // Close the modal
        setEditingTask(null);
        // Trigger dashboard update
        window.dispatchEvent(new CustomEvent('task-updated'));

        // Emit activity event for update
        const activity: ActivityItem = {
          id: `act-${Date.now()}-${Math.random().toString(36).substr(2, 9)}`,
          type: 'task_updated',
          taskId: updatedTask.id,
          taskTitle: updatedTask.title,
          userId: updatedTask.user_id,
          timestamp: new Date().toISOString(),
          message: `Task "${updatedTask.title}" was updated`
        };
        window.dispatchEvent(new CustomEvent('task-activity', { detail: activity }));
      } else {
        // Handle API-level error
        alert(result.error || "Failed to update task. Please try again.");
      }
    } catch (error) {
      // Handle network/error-level error
      console.error("Error updating task:", error);
      alert("Save failed. Please check your connection and try again.");
    }
  };

  if (loading) return (
    <div className="min-h-screen flex items-center justify-center bg-gradient-to-br from-teal-400 via-yellow-100 to-teal-600">
      <div className="animate-spin rounded-full h-12 w-12 border-b-2 border-black"></div>
    </div>
  );

  return (
    <div className="min-h-screen bg-gradient-to-br from-teal-400 via-yellow-100 to-teal-600 p-4">
      <div className="max-w-4xl mx-auto">
        {/* Header Section */}
        <header className="bg-white rounded-2xl shadow-xl p-6 mb-6">
          <div className="flex flex-col sm:flex-row justify-between items-start gap-4">
            <div>
              <h1 className="text-2xl font-bold text-teal-400">My Tasks</h1>
              <p className="text-gray-600">You have {tasks.filter(t => !t.completed).length} active tasks</p>
            </div>
            <div className="flex gap-3">
              <Link href="/dashboard" className="px-4 py-2 bg-teal-50 border border-teal-500 text-teal-500 rounded-lg hover:bg-teal-100 transition-all">Dashboard</Link>
              <Link href="/tasks/new" className="bg-gradient-to-br from-teal-400 via-yellow-200 to-teal-300 hover:from-teal-500 hover:via-yellow-300 hover:to-teal-400 text-black font-semibold px-4 py-2 rounded-lg shadow-md hover:opacity-90 transition-all">+ New Task</Link>
            </div>
          </div>

          {/* Filters */}
          <div className="mt-6 grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-3 gap-4">
            <input 
                type="text" placeholder="Search title or description..." 
                className="w-full px-4 py-2 text-gray-600 border border-teal-300 rounded-lg outline-none focus:ring-2 focus:ring-teal-400"
                value={searchTerm} onChange={e => setSearchTerm(e.target.value)}
            />
            <select className="w-full px-4 py-2 border border-teal-300 focus:ring-2 focus:ring-teal-400 focus:border-transparent outline-none transition-all hover rounded-lg" value={filter} onChange={e => setFilter(e.target.value as any)}>
                <option value="all">All Status</option>
                <option value="active">Active</option>
                <option value="completed">Completed</option>
                <option value="overdue">Overdue</option>
            </select>
            <div className="flex gap-2">
                <select className="flex-1 px-4 py-2 border border-teal-300 focus:ring-2 focus:ring-teal-400 focus:border-transparent outline-none transition-all rounded-lg" value={sortBy} onChange={e => setSortBy(e.target.value as any)}>
                    <option value="created_at">Created</option>
                    <option value="due_date">Due Date</option>
                    <option value="priority">Priority</option>
                </select>
                <button 
                    onClick={() => setSortOrder(prev => prev === 'asc' ? 'desc' : 'asc')}
                    className="p-2 border rounded-lg text-teal-500 bg-teal-50 hover:bg-teal-100"
                >
                    {sortOrder === 'asc' ? '↑' : '↓'}
                </button>
            </div>
          </div>
        </header>

        {/* Task List Container */}
        <main className="bg-white rounded-2xl shadow-xl p-6 min-h-[400px]">
          {filteredTasks.length === 0 ? (
            <div className="flex flex-col items-center justify-center py-20 text-center">
                <div className="text-5xl mb-4">📋</div>
                <h3 className="text-xl font-semibold text-gray-800">No tasks found</h3>
                <p className="text-gray-500">Adjust your filters or create a new one.</p>
            </div>
          ) : (
            <div className="space-y-4">
              {filteredTasks.map((task) => (
                <TaskItem 
                    key={task.id} 
                    task={task} 
                    onEdit={handleEditClick} 
                    onToggle={handleToggleComplete} 
                    onDelete={handleDeleteTask}
                />
              ))}
            </div>
          )}
        </main>
      </div>

      {/* Edit Task Modal */}
      {editingTask && (
        <div className="fixed inset-0 bg-black bg-opacity-50 flex items-center justify-center p-4 z-50">
          <div className="bg-white rounded-2xl shadow-xl w-full max-w-md p-6">
            <div className="flex justify-between items-center mb-4">
              <h2 className="text-xl font-bold text-teal-400">Edit Task</h2>
              <button
                onClick={() => setEditingTask(null)}
                className="text-gray-500 hover:text-gray-700"
              >
                ✕
              </button>
            </div>

            <div className="space-y-4">
              <div>
                <label htmlFor="edit-title" className="block text-sm font-medium text-gray-700 mb-1">
                  Title
                </label>
                <input
                  id="edit-title"
                  name="title"
                  type="text"
                  value={editFormData.title}
                  onChange={handleEditChange}
                  className="w-full px-4 py-2 text-gray-600 border border-teal-300 rounded-lg focus:ring-2 focus:ring-teal-400 focus:border-transparent outline-none transition-all"
                />
              </div>

              <div>
                <label htmlFor="edit-description" className="block text-sm font-medium text-gray-700 mb-1">
                  Description
                </label>
                <textarea
                  id="edit-description"
                  name="description"
                  value={editFormData.description}
                  onChange={handleEditChange}
                  rows={3}
                  className="w-full px-4 py-2 text-gray-600 border border-teal-300 rounded-lg focus:ring-2 focus:ring-teal-400 focus:border-transparent outline-none transition-all resize-none"
                />
              </div>

              <div className="grid grid-cols-2 gap-4">
                <div>
                  <label htmlFor="edit-priority" className="block text-sm font-medium text-gray-700 mb-1">
                    Priority
                  </label>
                  <select
                    id="edit-priority"
                    name="priority"
                    value={editFormData.priority}
                    onChange={handleEditChange}
                    className="w-full px-4 py-2 text-gray-600 border border-teal-300 rounded-lg focus:ring-2 focus:ring-teal-400 focus:border-transparent outline-none transition-all"
                  >
                    <option value="low">Low</option>
                    <option value="medium">Medium</option>
                    <option value="high">High</option>
                  </select>
                </div>

                <div>
                  <label htmlFor="edit-category" className="block text-sm font-medium text-gray-700 mb-1">
                    Category
                  </label>
                  <select
                    id="edit-category"
                    name="category"
                    value={editFormData.category}
                    onChange={handleEditChange}
                    className="w-full px-4 py-2 text-gray-600 border border-teal-300 rounded-lg focus:ring-2 focus:ring-teal-400 focus:border-transparent outline-none transition-all"
                  >
                    <option value="work">Work</option>
                    <option value="home">Home</option>
                    <option value="other">Other</option>
                  </select>
                </div>
              </div>

              <div className="grid grid-cols-2 gap-4">
                <div>
                  <label htmlFor="edit-due_date" className="text-gray-700 block text-sm font-medium mb-1">
                    Due Date
                  </label>
                  <input
                    id="edit-due_date"
                    name="due_date"
                    type="date"
                    value={editFormData.due_date}
                    onChange={handleEditChange}
                    className="w-full px-4 py-2 text-gray-600 border border-teal-300 rounded-lg focus:ring-2 focus:ring-teal-400 focus:border-transparent outline-none transition-all"
                  />
                </div>

                <div>
                  <label htmlFor="edit-recurring" className="text-gray-700 block text-sm font-medium mb-1">
                    Recurring
                  </label>
                  <select
                    id="edit-recurring"
                    name="recurring"
                    value={editFormData.recurring}
                    onChange={handleEditChange}
                    className="w-full px-4 py-2 border text-gray-600 border-teal-300 rounded-lg focus:ring-2 focus:ring-teal-400 focus:border-transparent outline-none transition-all"
                  >
                    <option value="none">None</option>
                    <option value="daily">Daily</option>
                    <option value="weekly">Weekly</option>
                    <option value="monthly">Monthly</option>
                  </select>
                </div>
              </div>
            </div>

            <div className="flex space-x-3 mt-6">
              <button
                onClick={() => setEditingTask(null)}
                className="flex-1 bg-gray-200 text-gray-800 py-2 rounded-lg font-medium hover:bg-gray-300 transition-all"
              >
                Cancel
              </button>
              <button
                onClick={handleSaveEdit}
                className="flex-1 bg-gradient-to-br from-teal-400 via-yellow-200 to-teal-300 text-black py-2 rounded-lg font-medium hover:from-teal-500 hover:via-yellow-300 hover:to-teal-400 transition-all"
              >
                Save
              </button>
            </div>
          </div>
        </div>
      )}
    </div>
  );
};

// Sub-component for cleaner mapping
const TaskItem = ({ task, onEdit, onToggle, onDelete }: { task: Task, onEdit: any, onToggle: any, onDelete: any }) => {
    const [showConfirmDelete, setShowConfirmDelete] = useState<number | null>(null);
    const today = new Date();
    today.setHours(0,0,0,0);

    let daysUntilDue: number | null = null;
    if (task.due_date) {
        // Handle both ISO string and date-only string formats
        let dueDate: Date;
        if (task.due_date.includes('T')) {
            dueDate = new Date(task.due_date);
        } else {
            // If it's just a date string (YYYY-MM-DD), treat as local date at midnight
            dueDate = new Date(`${task.due_date}T00:00:00`);
        }
        dueDate.setHours(0,0,0,0);
        daysUntilDue = Math.ceil((dueDate.getTime() - today.getTime()) / (1000 * 60 * 60 * 24));
    }

    // Determine category color
    const categoryColors = {
        work: 'bg-blue-100 text-blue-800',
        home: 'bg-purple-100 text-purple-800',
        other: 'bg-gray-100 text-gray-800'
    };

    // Determine recurring icon
    const recurringIcons = {
        daily: '📅',
        weekly: '📅',
        monthly: '📅',
        none: ''
    };

    const handleDeleteClick = () => {
        // Only show confirmation for this specific task
        setShowConfirmDelete(task.id);
    };

    const handleConfirmDelete = () => {
        onDelete(task.id);
        setShowConfirmDelete(null);
    };

    const handleCancelDelete = () => {
        setShowConfirmDelete(null);
    };

    return (
        <div className={`group border rounded-xl p-4 transition-all hover:shadow-md ${task.completed ? 'bg-gray-50 opacity-75' : 'bg-white border-gray-100'}`}>
            <div className="flex justify-between items-start">
                <div className="flex gap-3 items-start">
                    <input
                        type="checkbox"
                        checked={task.completed}
                        onChange={() => onToggle(task)}
                        className="mt-1.5 h-5 w-5 rounded border-purple-500 text-purple-600 focus:ring-purple-500 cursor-pointer"
                    />
                    <div>
                        <h3 className={`font-semibold text-lg ${task.completed ? 'line-through text-gray-400' : 'text-gray-800'}`}>
                            {task.title}
                        </h3>
                        {task.description && <p className="text-gray-500 text-sm mt-1">{task.description}</p>}

                        <div className="flex flex-wrap gap-2 mt-3">
                            <span className={`px-2 py-1 rounded-full text-xs font-medium ${
                                task.priority === 'high' ? 'bg-red-100 text-red-800' :
                                task.priority === 'medium' ? 'bg-yellow-100 text-yellow-800' :
                                'bg-green-100 text-green-800'
                            }`}>
                                {task.priority.charAt(0).toUpperCase() + task.priority.slice(1)}
                            </span>

                            {task.category && (
                                <span className={`px-2 py-1 rounded-full text-xs font-medium ${categoryColors[task.category]}`}>
                                    {task.category.charAt(0).toUpperCase() + task.category.slice(1)}
                                </span>
                            )}

                            {task.due_date && (
                                <span className={`px-2 py-1 rounded-full text-xs font-medium ${
                                    daysUntilDue === 0 ? 'bg-orange-100 text-orange-800' :
                                    daysUntilDue && daysUntilDue < 0 ? 'bg-red-100 text-red-800' :
                                    'bg-blue-100 text-blue-800'
                                }`}>
                                    {daysUntilDue === 0 ? 'Due Today' :
                                     daysUntilDue && daysUntilDue < 1 ? `Overdue ${Math.abs(daysUntilDue)} day` :
                                     `In ${daysUntilDue} day`}
                                </span>
                            )}

                            {task.recurring && task.recurring !== 'none' && (
                                <span className="px-2 py-1 rounded-full text-xs font-medium bg-indigo-100 text-indigo-800">
                                    {recurringIcons[task.recurring]} {task.recurring.charAt(0).toUpperCase() + task.recurring.slice(1)}
                                </span>
                            )}
                        </div>
                    </div>
                </div>
                <div className="flex flex-col items-end">
                    <div className="flex gap-2 opacity-0 group-hover:opacity-100 transition-opacity">
                        <button onClick={() => onEdit(task)} className="p-2 text-blue-600 hover:bg-blue-50 rounded-lg">Edit</button>
                        {showConfirmDelete === task.id ? (
                            <div className="flex gap-2">
                                <button
                                    onClick={handleConfirmDelete}
                                    className="p-2 text-red-600 hover:bg-red-50 rounded-lg font-medium"
                                >
                                    Confirm
                                </button>
                                <button
                                    onClick={handleCancelDelete}
                                    className="p-2 text-gray-600 hover:bg-gray-50 rounded-lg"
                                >
                                    Cancel
                                </button>
                            </div>
                        ) : (
                            <button onClick={handleDeleteClick} className="p-2 text-red-600 hover:bg-red-50 rounded-lg">Delete</button>
                        )}
                    </div>
                </div>
            </div>
        </div>
    );
}


export default TasksPage;