'use client';

import React, { useState, useEffect } from 'react';
import { useAuth } from '../../context/auth';
import { ActivityItem } from '../../types/activity';
import { formatTimeAgo } from '../../utils/timeUtils';
import { apiClient } from '../../api/client';

interface RecentActivityProps {
  className?: string;
}

const RecentActivity: React.FC<RecentActivityProps> = ({ className = '' }) => {
  const { token } = useAuth();
  const [activities, setActivities] = useState<ActivityItem[]>([]);
  const [loading, setLoading] = useState<boolean>(true);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    const fetchActivities = async () => {
      if (!token) {
        setError('Authentication token not available');
        setLoading(false);
        return;
      }

      try {
        setLoading(true);
        setError(null);

        const response = await apiClient.get<ActivityItem[]>('/activities/recent', {
          'Authorization': `Bearer ${token}`,
          'Content-Type': 'application/json',
        });

        if (!response.success) {
          throw new Error(`Failed to fetch activities: ${response.error}`);
        }

        // Limit to 10 most recent activities
        setActivities(response.data?.slice(0, 10) || []);
      } catch (err) {
        console.error('Error fetching recent activities:', err);
        setError(err instanceof Error ? err.message : 'Failed to load recent activities');
      } finally {
        setLoading(false);
      }
    };

    fetchActivities();

    // Listen for task activity events emitted by task operations
    const handleTaskActivity = (event: Event) => {
      const customEvent = event as CustomEvent;
      const activityDetail = customEvent.detail;

      // Convert the frontend event structure to match the backend API response structure
      const convertedActivity: ActivityItem = {
        id: activityDetail.id, // Could be string from frontend, but ActivityItem expects number
        action: activityDetail.type, // frontend uses 'type', backend uses 'action'
        task_title: activityDetail.taskTitle, // frontend uses 'taskTitle', backend uses 'task_title'
        created_at: activityDetail.timestamp // frontend uses 'timestamp', backend uses 'created_at'
      };

      // Add the new activity to the beginning of the list
      setActivities(prevActivities => {
        // Filter out duplicate activities (same ID) to avoid duplication
        const filteredActivities = prevActivities.filter(
          activity => activity.id !== convertedActivity.id
        );

        // Combine the new activity with existing ones and limit to 10
        return [convertedActivity, ...filteredActivities].slice(0, 10);
      });
    };

    window.addEventListener('task-activity', handleTaskActivity);

    // Clean up the event listener
    return () => {
      window.removeEventListener('task-activity', handleTaskActivity);
    };
  }, [token]);

  // Map actions to user-friendly messages
  const getActivityMessage = (activity: ActivityItem): string => {
    const { action, task_title } = activity;
    switch (action) {
      case 'task_created':
        return `Task '${task_title}' was created`;
      case 'task_updated':
        return `Task '${task_title}' was updated`;
      case 'task_deleted':
        return `Task '${task_title}' was deleted`;
      case 'task_completed':
        return `Task '${task_title}' was completed`;
      case 'task_uncompleted':
        return `Task '${task_title}' was marked as incomplete`;
      default:
        return `Task '${task_title}' was updated`;
    }
  };

  // Get icon and color based on action type
  const getActionIconAndColor = (action: string) => {
    switch (action) {
      case 'task_created':
        return { icon: '🟢', color: 'text-green-500' };
      case 'task_updated':
        return { icon: '🔵', color: 'text-blue-500' };
      case 'task_deleted':
        return { icon: '🔴', color: 'text-red-500' };
      case 'task_completed':
        return { icon: '🟣', color: 'text-indigo-500' };
      case 'task_uncompleted':
        return { icon: '🟡', color: 'text-yellow-500' };
      default:
        return { icon: '⚪', color: 'text-gray-500' };
    }
  };

  if (loading) {
    return (
      <div className={`bg-white rounded-xl shadow-md p-6 ${className}`}>
        <h3 className="text-lg font-semibold text-gray-800 mb-4">Recent Activity</h3>
        <div className="space-y-4">
          {[...Array(5)].map((_, index) => (
            <div key={index} className="animate-pulse flex items-start space-x-3">
              <div className="bg-gray-200 rounded-full h-6 w-6"></div>
              <div className="flex-1 space-y-2">
                <div className="h-4 bg-gray-200 rounded w-3/4"></div>
                <div className="h-3 bg-gray-200 rounded w-1/2"></div>
              </div>
            </div>
          ))}
        </div>
      </div>
    );
  }

  if (error) {
    return (
      <div className={`bg-white rounded-xl shadow-md p-6 ${className}`}>
        <h3 className="text-lg font-semibold text-gray-800 mb-4">Recent Activity</h3>
        <div className="text-red-500 text-center py-4">
          <p>Error loading activities</p>
          <p className="text-sm mt-1">{error}</p>
        </div>
      </div>
    );
  }

  if (activities.length === 0) {
    return (
      <div className={`bg-white rounded-xl shadow-md p-6 ${className}`}>
        <h3 className="text-lg font-semibold text-gray-800 mb-4">Recent Activity</h3>
        <div className="text-gray-500 text-center py-4">
          No recent activity in the last 24 hours
        </div>
      </div>
    );
  }

  return (
    <div className={`bg-white rounded-xl shadow-md p-6 ${className}`}>
      <div className="space-y-4">
        {activities.map((activity) => {
          const { icon, color } = getActionIconAndColor(activity.action);
          return (
            <div key={activity.id} className="flex items-start space-x-3">
              <div className={`${color} text-xl`}>{icon}</div>
              <div className="flex-1 min-w-0">
                <p className="text-sm text-gray-800 truncate">{getActivityMessage(activity)}</p>
                <p className="text-xs text-gray-500 mt-1">{formatTimeAgo(activity.created_at)}</p>
              </div>
            </div>
          );
        })}
      </div>
    </div>
  );
};

export default RecentActivity;