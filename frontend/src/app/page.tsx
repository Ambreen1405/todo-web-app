'use client';

import React, { useEffect } from 'react';
import { useRouter } from 'next/navigation';
import { useAuth } from './context/auth';

const HomePage: React.FC = () => {
  const router = useRouter();
  const { isAuthenticated, token } = useAuth();

  useEffect(() => {
    // Redirect to dashboard if authenticated, otherwise to login
    if (isAuthenticated) {
      // Trigger event to update dashboard stats after authentication check
      window.dispatchEvent(new CustomEvent('task-updated'));

      router.push('/dashboard');
    } else {
      router.push('/login');
    }
  }, [isAuthenticated, token, router]);

  return (
    <div className="min-h-screen flex items-center justify-center bg-gradient-to-br from-teal-400 via-yellow-200 to-teal-600 py-12 px-4 sm:px-6 lg:px-8">
      <div className="max-w-md w-full space-y-8">
        <div className='text-black'>
          <h2 className="mt-6 text-center text-3xl font-extrabold">
            Todo Application
          </h2>
          <p className="mt-2 text-center text-sm">
            Loading...
          </p>
        </div>
      </div>
    </div>
  );
};

export default HomePage;