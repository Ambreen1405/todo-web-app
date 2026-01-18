import { Metadata } from 'next';

export const metadata: Metadata = {
  title: 'Dashboard | TodoApp',
  description: 'Manage your tasks efficiently in your personalized dashboard. View, create, update, and organize your todos.',
  keywords: ['dashboard', 'tasks', 'todo', 'productivity', 'organizer'],
  openGraph: {
    title: 'Dashboard | TodoApp',
    description: 'Manage your tasks efficiently in your personalized dashboard. View, create, update, and organize your todos.',
    type: 'website',
    url: 'https://todoapp.example.com/dashboard',
  },
  twitter: {
    card: 'summary',
    title: 'Dashboard | TodoApp',
    description: 'Manage your tasks efficiently in your personalized dashboard.',
  },
  robots: {
    index: false, // Set to true in production
    follow: false, // Set to true in production
  },
};