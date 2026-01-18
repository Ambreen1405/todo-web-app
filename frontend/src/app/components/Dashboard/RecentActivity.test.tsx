import React from 'react';
import { render, screen, waitFor } from '@testing-library/react';
import { act } from 'react-dom/test-utils';
import RecentActivity from './RecentActivity';
import { ActivityItem, ActivityType } from '../../types/activity';

// Mock localStorage
const mockLocalStorage = (() => {
  let store: Record<string, string> = {};

  return {
    getItem: jest.fn((key: string) => store[key] || null),
    setItem: jest.fn((key: string, value: string) => {
      store[key] = value.toString();
    }),
    clear: jest.fn(() => {
      store = {};
    }),
  };
})();

Object.defineProperty(window, 'localStorage', {
  value: mockLocalStorage,
});

describe('RecentActivity Component', () => {
  beforeEach(() => {
    jest.clearAllMocks();
    (mockLocalStorage.getItem as jest.Mock).mockReturnValue(null);
  });

  test('renders without crashing', () => {
    render(<RecentActivity userId={1} />);
    expect(screen.getByText('No recent activity yet')).toBeInTheDocument();
  });

  test('shows recent activities when stored in localStorage', async () => {
    const activities: ActivityItem[] = [
      {
        id: '1',
        type: 'task_created' as ActivityType,
        taskId: 1,
        taskTitle: 'Test Task',
        userId: 1,
        timestamp: new Date().toISOString(),
        message: 'Task "Test Task" was created'
      }
    ];

    (mockLocalStorage.getItem as jest.Mock).mockReturnValue(JSON.stringify(activities));

    render(<RecentActivity userId={1} />);

    await waitFor(() => {
      expect(screen.getByText('Task "Test Task" was created')).toBeInTheDocument();
    });
  });

  test('handles activity events', async () => {
    render(<RecentActivity userId={1} />);

    // Dispatch a test activity event
    const testActivity: ActivityItem = {
      id: 'test-1',
      type: 'task_created' as ActivityType,
      taskId: 1,
      taskTitle: 'Test Task',
      userId: 1,
      timestamp: new Date().toISOString(),
      message: 'Task "Test Task" was created'
    };

    act(() => {
      window.dispatchEvent(new CustomEvent('task-activity', { detail: testActivity }));
    });

    await waitFor(() => {
      expect(screen.getByText('Task "Test Task" was created')).toBeInTheDocument();
    });
  });
});