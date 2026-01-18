'use client';

import React from 'react';
import { useRouter } from 'next/navigation';
import { useAuth } from '../../context/auth';

const Logout: React.FC = () => {
  const router = useRouter();
  const { logout } = useAuth();

  const handleLogout = () => {
    logout();
    router.push('/'); // Redirect to home page after logout
  };

  return (
    <button
      onClick={handleLogout}
      className="ml-4 px-4 py-2 text-sm font-medium text-white bg-red-600 rounded-md hover:bg-red-700 focus:outline-none focus:ring-2 focus:ring-offset-2 focus:ring-red-500"
    >
      Logout
    </button>
  );
};

export default Logout;