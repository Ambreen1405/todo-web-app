'use client';

import React, { createContext, useContext, useState, ReactNode } from 'react';

interface LoadingContextType {
  loading: boolean;
  setLoading: (loading: boolean) => void;
  showGlobalLoader: () => void;
  hideGlobalLoader: () => void;
}

const LoadingContext = createContext<LoadingContextType | undefined>(undefined);

interface LoadingProviderProps {
  children: ReactNode;
}

export const LoadingProvider: React.FC<LoadingProviderProps> = ({ children }) => {
  const [loading, setLoading] = useState(false);

  const showGlobalLoader = () => setLoading(true);
  const hideGlobalLoader = () => setLoading(false);

  return (
    <LoadingContext.Provider value={{
      loading,
      setLoading,
      showGlobalLoader,
      hideGlobalLoader
    }}>
      {children}
      {loading && (
        <div className="fixed inset-0 bg-black bg-opacity-25 flex items-center justify-center z-50">
          <div className="bg-white p-4 rounded-md shadow-lg flex items-center">
            <div className="animate-spin rounded-full h-6 w-6 border-b-2 border-blue-500 mr-3"></div>
            <span>Loading...</span>
          </div>
        </div>
      )}
    </LoadingContext.Provider>
  );
};

export const useLoading = () => {
  const context = useContext(LoadingContext);
  if (context === undefined) {
    throw new Error('useLoading must be used within a LoadingProvider');
  }
  return context;
};