# Authentication Integration Guide

This document explains how to integrate Better Auth into the existing todo-web-app project.

## Current State
The project currently uses a custom JWT-based authentication system with the following characteristics:
- Custom JWT token generation and validation
- Built-in user registration and login endpoints
- Session management using localStorage

## Better Auth Integration Steps

### 1. Install Better Auth Dependencies
```bash
# For the frontend
npm install better-auth

# For the backend (if using Node.js API routes)
npm install better-auth @better-auth/adapter-sqlite @better-auth/adapter-postgresql
```

### 2. Configure Better Auth
Create a Better Auth configuration file in the frontend (e.g., `lib/better-auth-client.ts`):

```typescript
import { createAuthClient } from "better-auth/client";

export const authClient = createAuthClient({
  baseURL: process.env.BETTER_AUTH_URL || "http://localhost:3000",
  signInMethods: ["email"],
});
```

### 3. Backend Configuration
If integrating with the existing FastAPI backend, you may need to either:
- Replace the current auth endpoints with Better Auth's API routes, or
- Use Better Auth in client-side mode only and keep the existing backend

### 4. Environment Variables in Use
The following environment variables have been added to support Better Auth:

#### .env
```
# Better Auth Configuration
BETTER_AUTH_SECRET=Lli0kaoi1N2l4UEpTiwK0WauXOvme2IK
BETTER_AUTH_URL=http://localhost:3000

# Database Configuration (Neon)
NEON_DATABASE_URL=psql 'postgresql://neondb_owner:npg_hAXfM7oN1pqV@ep-empty-recipe-ahl0tehx-pooler.c-3.us-east-1.aws.neon.tech/neondb?sslmode=require&channel_binding=require'
```

### 5. Update Frontend Components
Replace the current authentication context (`frontend/src/app/context/auth.tsx`) with Better Auth integration.

### 6. Database Migration
If using Better Auth's built-in database functionality, you may need to migrate from the current SQLModel-based user schema to Better Auth's schema.

## Important Notes
- The current application uses a FastAPI backend with SQLModel/SQLAlchemy for authentication
- Better Auth typically operates as a standalone authentication service
- Full integration may require significant refactoring of the existing authentication system
- Consider whether you want to use Better Auth as a replacement or alongside the existing system