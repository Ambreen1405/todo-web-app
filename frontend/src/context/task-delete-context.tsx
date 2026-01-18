'use client';

import React, { createContext, useContext, useState, ReactNode } from 'react';

interface TaskDeleteContextType {
  confirmingTaskId: number | null;
  setConfirmingTaskId: (id: number | null) => void;
}

const TaskDeleteContext = createContext<TaskDeleteContextType | undefined>(undefined);

export const TaskDeleteProvider: React.FC<{ children: ReactNode }> = ({ children }) => {
  const [confirmingTaskId, setConfirmingTaskId] = useState<number | null>(null);

  return (
    <TaskDeleteContext.Provider value={{ confirmingTaskId, setConfirmingTaskId }}>
      {children}
    </TaskDeleteContext.Provider>
  );
};

export const useTaskDelete = (): TaskDeleteContextType => {
  const context = useContext(TaskDeleteContext);
  if (context === undefined) {
    throw new Error('useTaskDelete must be used within a TaskDeleteProvider');
  }
  return context;
};