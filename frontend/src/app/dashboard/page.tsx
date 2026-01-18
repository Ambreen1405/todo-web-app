"use client";

import React, { useEffect, useState, useCallback } from "react";
import { useRouter } from "next/navigation";
import { useAuth } from "../context/auth";
import Link from "next/link";
import { tasksApi } from "../api/tasks";
import { Task } from "../types/task";
import RecentActivity from "../components/Dashboard/RecentActivity";

const DashboardPage: React.FC = () => {
  const router = useRouter();
  const { user, token, isAuthenticated, logout } = useAuth();
  const [loading, setLoading] = useState(true);
  const [stats, setStats] = useState({
    total: 0,
    completed: 0,
    pending: 0,
  });

  const fetchTaskStats = useCallback(async () => {
    if (!isAuthenticated || !token) return;

    try {
      const response = await tasksApi.getAllTasks(token);
      if (response.success && response.tasks) {
        const tasks = response.tasks as Task[];
        const completedCount = tasks.filter((task) => task.completed).length;
        const pendingCount = tasks.length - completedCount;

        setStats({
          total: tasks.length,
          completed: completedCount,
          pending: pendingCount,
        });
      } else {
        // Set to 0 if there's an error or no tasks
        setStats({
          total: 0,
          completed: 0,
          pending: 0,
        });
      }
    } catch (error) {
      console.error("Error fetching tasks:", error);
      // Set to 0 on error
      setStats({
        total: 0,
        completed: 0,
        pending: 0,
      });
    }
  }, [isAuthenticated, token]);

  useEffect(() => {
    // Check authentication status
    if (!isAuthenticated || !token) {
      router.push("/login");
      return;
    }

    // Fetch task statistics
    fetchTaskStats().finally(() => {
      setLoading(false);
    });

    // Refresh stats when the component mounts
  }, [isAuthenticated, token, router, fetchTaskStats]);

  // Refresh stats when tasks are updated (like after creating a new task)
  useEffect(() => {
    const handleTaskUpdated = () => {
      fetchTaskStats();
    };

    window.addEventListener("task-updated", handleTaskUpdated);

    return () => {
      window.removeEventListener("task-updated", handleTaskUpdated);
    };
  }, [fetchTaskStats]);

  const handleLogout = () => {
    logout();
    router.push("/login");
  };

  if (loading) {
    return (
      <div className="min-h-screen flex items-center justify-center bg-gradient-to-br from-teal-400 via-yellow-100 to-teal-600 p-4">
        <div className="text-black text-center">
          <div className="animate-spin rounded-full h-12 w-12 border-b-2 border-black mx-auto"></div>
          <p className="mt-4 text-lg">Loading dashboard...</p>
        </div>
      </div>
    );
  }

  return (
    <div className="min-h-screen bg-gradient-to-br from-teal-400 via-yellow-100 to-teal-600 p-4">
      <div className="max-w-4xl mx-auto">
        {/* Header */}
        <header className="bg-white rounded-2xl shadow-xl p-6 mb-6">
          <div className="flex flex-col sm:flex-row sm:justify-between sm:items-center gap-4">
            <div>
              <h1 className="text-2xl font-bold text-teal-500">
                Todo Dashboard
              </h1>
              <p className="text-gray-600">
                Welcome back, {user?.name || "User"}!
              </p>
            </div>
            <div className="flex items-center justify-between sm:justify-end space-x-4">
              <button
                onClick={handleLogout}
                className="flex items-center gap-1 sm:gap-2 px-3 sm:px-4 py-1 rounded-full bg-gradient-to-r from-pink-500 to-red-500 text-white font-bold tracking-wide shadow-md hover:scale-105 hover:shadow-xl transition-all duration-200 whitespace-nowrap">
                <span className="text-[0.6rem] sm:text-[0.8rem]">LOG OUT</span>
                <span className="bg-white/20 p-1 text-[0.7rem] sm:text-[0.8rem] rounded-full">🔒</span>
              </button>
            </div>
          </div>
        </header>

        {/* Main Content */}
        <main className="grid grid-cols-1 lg:grid-cols-3 gap-6">
          {/* Stats Cards - shown after quick actions on mobile, on left side on desktop */}
          <div className="lg:col-span-2 grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-3 gap-4">
            <div className="bg-white rounded-2xl shadow-lg p-6">
              <h3 className="text-lg font-semibold text-gray-800">
                Total Tasks
              </h3>
              <p className="text-3xl font-bold text-teal-600 mt-2">
                {stats.total}
              </p>
              <p className="text-gray-600 text-sm mt-1">
                {stats.total === 0
                  ? "No tasks yet"
                  : stats.total === 1
                  ? "1 task"
                  : `${stats.total} tasks`}
              </p>
            </div>
            <div className="bg-white rounded-2xl shadow-lg p-6">
              <h3 className="text-lg font-semibold text-gray-800">Completed</h3>
              <p className="text-3xl font-bold text-green-600 mt-2">
                {stats.completed}
              </p>
              <p className="text-gray-600 text-sm mt-1">
                {stats.completed === 0
                  ? "None completed yet"
                  : stats.completed === 1
                  ? "1 completed"
                  : `${stats.completed} completed`}
              </p>
            </div>
            <div className="bg-white rounded-2xl shadow-lg p-6">
              <h3 className="text-lg font-semibold text-gray-800">Pending</h3>
              <p className="text-3xl font-bold text-yellow-600 mt-2">
                {stats.pending}
              </p>
              <p className="text-gray-600 text-sm mt-1">
                {stats.pending === 0
                  ? "All caught up!"
                  : stats.pending === 1
                  ? "1 pending"
                  : `${stats.pending} pending`}
              </p>
            </div>
          </div>

          {/* Quick Actions - shown first on mobile, then on right side on desktop */}
          <div className="order-first lg:order-last bg-white rounded-2xl shadow-lg p-6">
            <h3 className="text-lg font-semibold text-gray-800 mb-4">
              Quick Actions
            </h3>
            <div className="space-y-3">
              <Link
                href="/tasks"
                className="block w-full bg-gradient-to-br from-teal-400 via-yellow-200 to-teal-300 text-black font-semibold py-3 rounded-lg text-center hover:from-teal-500 hover:via-yellow-300 hover:to-teal-400 transition-all"
              >
                View Tasks
              </Link>
              <Link
                href="/tasks/new"
                className="block w-full bg-gray-100 text-gray-800 py-3 rounded-lg font-medium text-center hover:bg-gray-200 transition-all"
              >
                Create New Task
              </Link>
            </div>
          </div>
        </main>

        {/* Recent Activity */}
        <div className="bg-white rounded-2xl shadow-lg p-6 mt-6">
          <h3 className="text-lg font-semibold text-gray-800 mb-4">Recent Activity</h3>
          {user ? (
            <RecentActivity />
          ) : (
            <div className="text-center py-8 text-gray-500">
              <p>Loading activity...</p>
            </div>
          )}
        </div>
      </div>
    </div>
  );
};

export default DashboardPage;
