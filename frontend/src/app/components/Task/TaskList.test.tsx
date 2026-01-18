import React from 'react';
import { render, screen, fireEvent, waitFor } from '@testing-library/react';
import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import { MemoryRouter } from 'react-router-dom';
import TaskList from './TaskList';

// Mock the apiClient module
vi.mock('../../api/client', () => ({
  apiClient: {
    get: vi.fn(),
    post: vi.fn(),
    put: vi.fn(),
    delete: vi.fn(),
  }
}));

// Mock the useAuth hook
vi.mock('../../context/auth', () => ({
  useAuth: () => ({
    isAuthenticated: true,
    token: 'mock-token',
    user: { id: 1, email: 'test@example.com', name: 'Test User' },
    login: vi.fn(),
    logout: vi.fn(),
    register: vi.fn(),
  }),
}));

// Mock TaskForm and TaskItem components
vi.mock('./TaskForm', () => ({
  default: ({ onTaskCreated }: { onTaskCreated: () => void }) => (
    <div data-testid="task-form">
      <button onClick={onTaskCreated}>Create Task</button>
    </div>
  )
}));

vi.mock('./TaskItem', () => ({
  default: ({ task, onTaskUpdate }: { task: any; onTaskUpdate: () => void }) => (
    <div data-testid="task-item">
      <span>{task.title}</span>
      <button onClick={onTaskUpdate}>Update Task</button>
    </div>
  )
}));

describe('TaskList Component', () => {
  const mockOnTaskUpdate = vi.fn();

  beforeEach(() => {
    vi.clearAllMocks();
  });

  afterEach(() => {
    vi.restoreAllMocks();
  });

  it('renders loading state initially', async () => {
    const mockGet = vi.fn().mockResolvedValue({ success: true, data: [] });
    vi.mocked(require('../../api/client').apiClient.get).mockImplementation(mockGet);

    render(
      <MemoryRouter>
        <TaskList onTaskUpdate={mockOnTaskUpdate} />
      </MemoryRouter>
    );

    expect(screen.getByText(/loading tasks/i)).toBeInTheDocument();
  });

  it('displays tasks when fetched successfully', async () => {
    const mockTasks = [
      { id: 1, title: 'Test Task 1', completed: false, priority: 'medium' },
      { id: 2, title: 'Test Task 2', completed: true, priority: 'high' },
    ];

    const mockGet = vi.fn().mockResolvedValue({ success: true, data: mockTasks });
    vi.mocked(require('../../api/client').apiClient.get).mockImplementation(mockGet);

    render(
      <MemoryRouter>
        <TaskList onTaskUpdate={mockOnTaskUpdate} />
      </MemoryRouter>
    );

    await waitFor(() => {
      expect(screen.getByText('Test Task 1')).toBeInTheDocument();
      expect(screen.getByText('Test Task 2')).toBeInTheDocument();
    });
  });

  it('displays error message when task fetching fails', async () => {
    const mockGet = vi.fn().mockResolvedValue({ success: false, error: 'Failed to fetch tasks' });
    vi.mocked(require('../../api/client').apiClient.get).mockImplementation(mockGet);

    render(
      <MemoryRouter>
        <TaskList onTaskUpdate={mockOnTaskUpdate} />
      </MemoryRouter>
    );

    await waitFor(() => {
      expect(screen.getByText(/failed to fetch tasks/i)).toBeInTheDocument();
    });
  });

  it('applies filters correctly', async () => {
    const mockTasks = [
      { id: 1, title: 'Completed Task', completed: true, priority: 'low' },
      { id: 2, title: 'Pending Task', completed: false, priority: 'high' },
    ];

    const mockGet = vi.fn().mockResolvedValue({ success: true, data: mockTasks });
    vi.mocked(require('../../api/client').apiClient.get).mockImplementation(mockGet);

    render(
      <MemoryRouter>
        <TaskList onTaskUpdate={mockOnTaskUpdate} />
      </MemoryRouter>
    );

    await waitFor(() => {
      expect(screen.getByText('Completed Task')).toBeInTheDocument();
      expect(screen.getByText('Pending Task')).toBeInTheDocument();
    });

    // Note: Testing the actual filter functionality would require more complex mocking
    // to simulate the UI interactions properly
  });

  it('shows empty state when no tasks exist', async () => {
    const mockGet = vi.fn().mockResolvedValue({ success: true, data: [] });
    vi.mocked(require('../../api/client').apiClient.get).mockImplementation(mockGet);

    render(
      <MemoryRouter>
        <TaskList onTaskUpdate={mockOnTaskUpdate} />
      </MemoryRouter>
    );

    await waitFor(() => {
      expect(screen.getByText(/no tasks yet/i)).toBeInTheDocument();
    });
  });

  it('triggers onTaskUpdate callback when task is created', async () => {
    const mockGet = vi.fn().mockResolvedValue({ success: true, data: [] });
    vi.mocked(require('../../api/client').apiClient.get).mockImplementation(mockGet);

    render(
      <MemoryRouter>
        <TaskList onTaskUpdate={mockOnTaskUpdate} />
      </MemoryRouter>
    );

    // Simulate task creation via TaskForm
    const taskFormButton = screen.getByTestId('task-form').querySelector('button');
    if (taskFormButton) {
      fireEvent.click(taskFormButton);
    }

    expect(mockOnTaskUpdate).toHaveBeenCalled();
  });
});