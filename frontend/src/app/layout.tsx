import React from 'react';
import './globals.css';
import type { Metadata } from 'next';
import { Inter } from 'next/font/google';
import { AuthProvider } from './context/auth';
import { LoadingProvider } from './context/loading';
import { TaskDeleteProvider } from '../context/task-delete-context';
import ErrorBoundary from './components/ErrorBoundary';

const inter = Inter({ subsets: ['latin'] });

export const metadata: Metadata = {
  title: {
    default: 'TodoApp - Secure Task Management',
    template: '%s | TodoApp'
  },
  description: 'A secure, full-featured todo application with user authentication and task management. Organize your tasks efficiently and securely.',
  keywords: ['todo', 'task management', 'productivity', 'secure app', 'authentication'],
  authors: [{ name: 'TodoApp Team' }],
  creator: 'TodoApp Team',
  publisher: 'TodoApp Team',
  metadataBase: new URL(process.env.NEXT_PUBLIC_APP_URL || 'http://localhost:3000'),
  formatDetection: {
    email: false,
    address: false,
    telephone: false,
  },
  openGraph: {
    type: 'website',
    locale: 'en_US',
    url: 'https://todoapp.example.com',
    title: 'TodoApp - Secure Task Management',
    description: 'A secure, full-featured todo application with user authentication and task management.',
    siteName: 'TodoApp',
  },
  twitter: {
    card: 'summary_large_image',
    title: 'TodoApp - Secure Task Management',
    description: 'A secure, full-featured todo application with user authentication and task management.',
  },
  robots: {
    index: false, // Set to true in production
    follow: false, // Set to true in production
    nocache: true,
    googleBot: {
      index: false, // Set to true in production
      follow: false, // Set to true in production
      noimageindex: true,
    },
  },
  icons: {
    icon: '/favicon.ico',
  },
};

export default function RootLayout({
  children,
}: {
  children: React.ReactNode;
}) {
  return (
    <html lang="en">
      <body className={inter.className}>
        <AuthProvider>
          <LoadingProvider>
            <TaskDeleteProvider>
              <ErrorBoundary>
                <div id="root">{children}</div>
              </ErrorBoundary>
            </TaskDeleteProvider>
          </LoadingProvider>
        </AuthProvider>
      </body>
    </html>
  );
}