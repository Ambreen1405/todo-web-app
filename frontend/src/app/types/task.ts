export interface Task {
  id: number;
  title: string;
  description?: string;
  completed: boolean;
  priority: 'low' | 'medium' | 'high';
  due_date?: string; // ISO date string
  created_at: string; // ISO date string
  updated_at: string; // ISO date string
  user_id: number;
  recurring?: 'daily' | 'weekly' | 'monthly' | 'none'; // Recurring option
  category?: 'work' | 'home' | 'other'; // Category
}