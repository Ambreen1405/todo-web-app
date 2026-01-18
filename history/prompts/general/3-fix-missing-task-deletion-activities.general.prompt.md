---
id: 3
title: Fix Missing Task Deletion Activities in Recent Activity
stage: general
date_iso: 2026-01-17
surface: agent
model: claude-sonnet-4-5-20250929
feature: none
branch: main
user: Claude Sonnet 4.5
command: "Fix missing task deletion activities in Recent Activity"
labels: ["bug-fix", "activity-tracking", "real-time-events", "dashboard"]
links:
  spec: null
  ticket: null
  adr: null
  pr: null
files_yaml:
  - "frontend/src/app/types/activity.ts"
  - "frontend/src/app/components/Dashboard/RecentActivity.tsx"
tests_yaml: []
---

# Fix Missing Task Deletion Activities in Recent Activity

## User Prompt
In the TodoApp, when I perform any action like create, update, or complete, it shows in Recent Activity, but when I delete a task, the delete action does not appear in Recent Activity.

## Response Text
Fixed the issue where task deletion activities were not appearing in the Recent Activity feed. The problem was that while the backend properly logged deletion activities, the RecentActivity component wasn't listening to the real-time events emitted by frontend task operations.

## Root Cause Analysis

1. The backend properly logged all task activities including deletions in the database
2. The frontend TaskItem component was emitting deletion events via `window.dispatchEvent(new CustomEvent('task-activity', ...))`
3. However, the RecentActivity component only fetched activities from the backend API periodically and wasn't listening to the real-time events
4. There was also a type mismatch between frontend events (using different field names) and backend API responses

## Changes Made

### Updated Activity Type Definition (`frontend/src/app/types/activity.ts`)
- Changed `id` field from `number` to `number | string` to accommodate both backend (number IDs) and frontend event (string IDs) formats
- Added explicit `ActivityType` type definition to resolve undefined type issues

### Updated RecentActivity Component (`frontend/src/app/components/Dashboard/RecentActivity.tsx`)
- Added event listener for `task-activity` custom events dispatched by task operations
- Implemented conversion logic to map frontend event structure to backend API response structure
- Added logic to add new activities to the top of the list without duplicates
- Included proper cleanup of event listeners to prevent memory leaks

## Result
Task deletion activities now appear in real-time in the Recent Activity feed alongside create, update, and complete activities. The implementation handles both the periodic API fetch and real-time event updates seamlessly.

## Follow-up
The RecentActivity component now properly synchronizes with all task operations (create, update, complete, delete) in real-time while continuing to periodically fetch from the backend API for consistency.