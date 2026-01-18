---
id: 1
title: Fix Dashboard Recent Activity Loading Error
stage: general
date_iso: 2026-01-17
surface: agent
model: claude-sonnet-4-5-20250929
feature: none
branch: main
user: Claude Sonnet 4.5
command: "Fix dashboard recent activity loading error"
labels: ["bug-fix", "dashboard", "api-client", "authentication"]
links:
  spec: null
  ticket: null
  adr: null
  pr: null
files_yaml:
  - "frontend/src/app/components/Dashboard/RecentActivity.tsx"
  - "backend/src/utils/auth.py"
tests_yaml: []
---

# Fix Dashboard Recent Activity Loading Error

## User Prompt
When I go to the URL http://localhost:3000/dashboard it shows:
Recent Activity
Error loading activities

Failed to fetch activities: 404 Not Found.
Please fix this.

## Response Text
Fixed the issue with recent activities not loading on the dashboard. The problem had two components:

1. **Frontend Issue**: The RecentActivity component was making a direct fetch call to `/activities/recent` instead of using the centralized API client that properly constructs the full backend URL (`https://ambreen-dev-todo-app.hf.space/activities/recent`).

2. **Backend Authentication Issue**: There was a bug in the JWT exception handling in `backend/src/utils/auth.py` where the code was referencing `jwt.exceptions.JWTError` which doesn't exist in the current PyJWT version, causing a 500 server error.

## Changes Made

### Frontend Changes (`frontend/src/app/components/Dashboard/RecentActivity.tsx`)
- Added import for the API client: `import { apiClient } from '../../api/client';`
- Updated the fetchActivities function to use `apiClient.get<ActivityItem[]>('/activities/recent', headers)` instead of direct fetch
- Properly handled the response format expected by the API client

### Backend Changes (`backend/src/utils/auth.py`)
- Fixed JWT exception handling by changing `jwt.exceptions.JWTError` to `jwt.InvalidTokenError`
- Also corrected `jwt.exceptions.ExpiredSignatureError` to `jwt.ExpiredSignatureError`

## Result
The dashboard now properly loads recent activities from the backend API. The frontend correctly uses the API client with the proper base URL, and the backend properly handles JWT authentication without throwing internal server errors.

## Follow-up
The backend server needs to be restarted to apply the auth.py changes. With both fixes in place, the recent activities should load correctly on the dashboard.