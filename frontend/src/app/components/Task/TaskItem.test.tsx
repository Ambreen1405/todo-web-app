import React from 'react';
import { render, screen, fireEvent, waitFor } from '@testing-library/react';
import { describe, it, expect, vi } from 'vitest';
import { MemoryRouter } from 'react-router-dom';
import TaskItem from './TaskItem';

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

describe('TaskItem Component', () => {
  const mockTask = {
    id: 1,
    title: 'Test Task',
    description: 'Test Description',
    completed: false,
    priority: 'medium',
    due_date: '2023-12-31',
    created_at: '2023-01-01T00:00:00Z',
    updated_at: '2023-01-01T00:00:00Z',
    user_id: 1,
  };

  const mockOnTaskUpdate = vi.fn();

  it('renders task information correctly', () => {
    render(
      <MemoryRouter>
        <TaskItem task={mockTask} onTaskUpdate={mockOnTaskUpdate} />
      </MemoryRouter>
    );

    expect(screen.getByText('Test Task')).toBeInTheDocument();
    expect(screen.getByText('Test Description')).toBeInTheDocument();
    expect(screen.getByText(/due: \d{1,2}\/\d{1,2}\/\d{4}/i)).toBeInTheDocument(); // Date format
    expect(screen.getByText('medium')).toBeInTheDocument();
  });

  it('allows toggling task completion', async () => {
    const mockPut = vi.fn().mockResolvedValue({ success: true });
    vi.mocked(require('../../api/client').apiClient.put).mockImplementation(mockPut);

    render(
      <MemoryRouter>
        <TaskItem task={mockTask} onTaskUpdate={mockOnTaskUpdate} />
      </MemoryRouter>
    );

    const checkbox = screen.getByRole('checkbox');
    expect(checkbox).not.toBeChecked();

    fireEvent.click(checkbox);

    await waitFor(() => {
      expect(mockPut).toHaveBeenCalledWith(
        `/tasks/${mockTask.id}`,
        { completed: true },
        expect.any(Object)
      );
    });
  });

  it('enters edit mode when edit button is clicked', () => {
    render(
      <MemoryRouter>
        <TaskItem task={mockTask} onTaskUpdate={mockOnTaskUpdate} />
      </MemoryRouter>
    );

    const editButton = screen.getByText('Edit');
    fireEvent.click(editButton);

    expect(screen.getByDisplayValue('Test Task')).toBeInTheDocument();
    expect(screen.getByDisplayValue('Test Description')).toBeInTheDocument();
  });

  it('shows delete confirmation and deletes task', async () => {
    const mockDelete = vi.fn().mockResolvedValue({ success: true });
    vi.mocked(require('../../api/client').apiClient.delete).mockImplementation(mockDelete);

    // Mock window.confirm to return true
    Object.defineProperty(window, 'confirm', {
      writable: true,
      value: vi.fn(() => true),
    });

    render(
      <MemoryRouter>
        <TaskItem task={mockTask} onTaskUpdate={mockOnTaskUpdate} />
      </MemoryRouter>
    );

    const deleteButton = screen.getByText('Delete');
    fireEvent.click(deleteButton);

    await waitFor(() => {
      expect(mockDelete).toHaveBeenCalledWith(
        `/tasks/${mockTask.id}`,
        expect.any(Object)
      );
      expect(mockOnTaskUpdate).toHaveBeenCalled();
    });
  });

  it('completes task editing successfully', async () => {
    const mockPut = vi.fn().mockResolvedValue({ success: true });
    vi.mocked(require('../../api/client').apiClient.put).mockImplementation(mockPut);

    render(
      <MemoryRouter>
        <TaskItem task={mockTask} onTaskUpdate={mockOnTaskUpdate} />
      </MemoryRouter>
    );

    // Enter edit mode
    const editButton = screen.getByText('Edit');
    fireEvent.click(editButton);

    // Change task title
    const titleInput = screen.getByDisplayValue('Test Task');
    fireEvent.change(titleInput, { target: { value: 'Updated Task' } });

    // Click save
    const saveButton = screen.getByText('Save');
    fireEvent.click(saveButton);

    await waitFor(() => {
      expect(mockPut).toHaveBeenCalledWith(
        `/tasks/${mockTask.id}`,
        {
          title: 'Updated Task',
          description: 'Test Description',
          due_date: null,
          completed: false
        },
        expect.any(Object)
      );
      expect(mockOnTaskUpdate).toHaveBeenCalled();
    });
  });

  it('displays completed tasks with strikethrough', () => {
    const completedTask = { ...mockTask, completed: true };

    render(
      <MemoryRouter>
        <TaskItem task={completedTask} onTaskUpdate={mockOnTaskUpdate} />
      </MemoryRouter>
    );

    const taskTitle = screen.getByText('Test Task');
    expect(taskTitle).toHaveClass('line-through');
  });
});