import { Metadata } from 'next';
import { redirect } from 'next/navigation';
import { getAuthSession } from '../../utils/auth';
import Logout from '../../components/Auth/Logout';
import TaskList from '../../components/Task/TaskList';

export async function generateMetadata(): Promise<Metadata> {
  return {
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
}

export default async function DashboardPage() {
  // Server-side authentication check
  const session = await getAuthSession();

  if (!session) {
    // Redirect to login if not authenticated
    redirect('/login');
  }

  return (
    <div className="min-h-screen bg-gray-50">
      <nav className="bg-white shadow">
        <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8">
          <div className="flex justify-between h-16">
            <div className="flex">
              <div className="flex-shrink-0 flex items-center">
                <span className="text-xl font-bold text-gray-900">Todo App</span>
              </div>
            </div>
            <div className="flex items-center">
              <div className="text-sm text-gray-700 mr-4">
                Welcome, {session.user?.name || 'User'}!
              </div>
              <Logout />
            </div>
          </div>
        </div>
      </nav>

      <div className="max-w-7xl mx-auto py-6 sm:px-6 lg:px-8">
        <div className="px-4 py-6 sm:px-0">
          <div className="pb-16">
            <div className="text-center mb-8">
              <h1 className="text-3xl font-bold text-gray-900">Your Dashboard</h1>
              <p className="mt-2 text-gray-600">
                Manage your tasks efficiently
              </p>
            </div>

            <TaskList onTaskUpdate={() => {}} />
          </div>
        </div>
      </div>
    </div>
  );
}