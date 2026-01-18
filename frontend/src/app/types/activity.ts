export interface ActivityItem {
  id: number | string; // Backend returns number, frontend events use string
  action?: 'task_created' | 'task_updated' | 'task_deleted' | 'task_completed' | 'task_uncompleted'; // Backend action type
  type?: 'task_created' | 'task_updated' | 'task_deleted' | 'task_completed' | 'task_uncompleted'; // Frontend event type
  taskId?: number; // ID of the associated task
  userId?: number; // ID of the user who performed the action
  task_title?: string; // Backend field (snake_case)
  taskTitle?: string; // Frontend event property name (camelCase)
  created_at?: string; // Backend timestamp field (snake_case)
  timestamp?: string; // Frontend event property name (camelCase)
  message?: string; // Activity message
}

// Define ActivityType as a type alias for the action field
export type ActivityType = 'task_created' | 'task_updated' | 'task_deleted' | 'task_completed' | 'task_uncompleted';

export interface ActivityResponse extends ActivityItem {}