# Todo Web App - API Documentation

## Overview
This document describes the API endpoints for the Todo Web Application. The application provides user authentication and task management functionality with secure access controls.

## Base URL
- Development: `https://ambreen-dev-todo-app.hf.space`
- Production: `https://api.yourdomain.com`

## Authentication
All protected endpoints require a valid JWT token in the Authorization header:
```
Authorization: Bearer <jwt_token>
```

## Common Headers
- `Content-Type: application/json`
- `Accept: application/json`

## Common Response Formats

### Success Response
```json
{
  "success": true,
  "data": { ... },
  "message": "Operation successful"
}
```

### Error Response
```json
{
  "success": false,
  "error": "Error message",
  "details": { ... }
}
```

---

## Authentication Endpoints

### POST /auth/register
Register a new user account.

#### Request Body
```json
{
  "email": "user@example.com",
  "name": "John Doe",
  "password": "securePassword123"
}
```

#### Validation
- Email must be a valid email address
- Name must be at least 1 character
- Password must be at least 8 characters

#### Responses
- `200 OK`: User registered successfully
- `400 Bad Request`: Invalid input data
- `409 Conflict`: User with email already exists

### POST /auth/login
Authenticate a user and return a JWT token.

#### Request Body
```json
{
  "email": "user@example.com",
  "password": "securePassword123"
}
```

#### Responses
- `200 OK`: Login successful, returns token
  ```json
  {
    "access_token": "jwt_token_here",
    "token_type": "bearer"
  }
  ```
- `400 Bad Request`: Invalid input data
- `401 Unauthorized`: Invalid credentials

### GET /auth/me
Get current user information (requires authentication).

#### Headers
```
Authorization: Bearer <valid_jwt_token>
```

#### Responses
- `200 OK`: User data retrieved successfully
  ```json
  {
    "id": 1,
    "email": "user@example.com",
    "name": "John Doe",
    "created_at": "2023-01-01T00:00:00Z"
  }
  ```
- `401 Unauthorized`: Invalid or expired token

### POST /auth/logout
Logout the current user (placeholder implementation).

#### Headers
```
Authorization: Bearer <valid_jwt_token>
```

#### Responses
- `200 OK`: Logout successful
- `401 Unauthorized`: Invalid or expired token

---

## Task Management Endpoints

### GET /tasks
Retrieve a list of tasks for the authenticated user.

#### Query Parameters
- `skip` (optional): Number of records to skip (default: 0)
- `limit` (optional): Maximum number of records to return (default: 100)
- `status` (optional): Filter by task status ('pending', 'completed')
- `priority` (optional): Filter by priority ('low', 'medium', 'high')

#### Headers
```
Authorization: Bearer <valid_jwt_token>
```

#### Responses
- `200 OK`: Tasks retrieved successfully
  ```json
  {
    "tasks": [
      {
        "id": 1,
        "title": "Complete project",
        "description": "Finish the todo app project",
        "status": "pending",
        "priority": "high",
        "due_date": "2023-12-31T23:59:59Z",
        "created_at": "2023-01-01T00:00:00Z",
        "updated_at": "2023-01-01T00:00:00Z"
      }
    ],
    "total": 1,
    "skip": 0,
    "limit": 100
  }
  ```
- `401 Unauthorized`: Invalid or expired token

### POST /tasks
Create a new task for the authenticated user.

#### Request Body
```json
{
  "title": "Complete project",
  "description": "Finish the todo app project",
  "status": "pending",
  "priority": "high",
  "due_date": "2023-12-31T23:59:59Z"
}
```

#### Validation
- Title is required and must be 1-100 characters
- Status must be 'pending' or 'completed'
- Priority must be 'low', 'medium', or 'high'
- Due date must be in ISO 8601 format (optional)

#### Headers
```
Authorization: Bearer <valid_jwt_token>
```

#### Responses
- `201 Created`: Task created successfully
  ```json
  {
    "id": 1,
    "title": "Complete project",
    "description": "Finish the todo app project",
    "status": "pending",
    "priority": "high",
    "due_date": "2023-12-31T23:59:59Z",
    "created_at": "2023-01-01T00:00:00Z",
    "updated_at": "2023-01-01T00:00:00Z"
  }
  ```
- `400 Bad Request`: Invalid input data
- `401 Unauthorized`: Invalid or expired token

### GET /tasks/{task_id}
Retrieve a specific task by ID.

#### Path Parameters
- `task_id`: Integer ID of the task to retrieve

#### Headers
```
Authorization: Bearer <valid_jwt_token>
```

#### Responses
- `200 OK`: Task retrieved successfully
  ```json
  {
    "id": 1,
    "title": "Complete project",
    "description": "Finish the todo app project",
    "status": "pending",
    "priority": "high",
    "due_date": "2023-12-31T23:59:59Z",
    "created_at": "2023-01-01T00:00:00Z",
    "updated_at": "2023-01-01T00:00:00Z"
  }
  ```
- `401 Unauthorized`: Invalid or expired token
- `404 Not Found`: Task not found or belongs to another user

### PUT /tasks/{task_id}
Update an existing task.

#### Path Parameters
- `task_id`: Integer ID of the task to update

#### Request Body
```json
{
  "title": "Updated task title",
  "description": "Updated task description",
  "status": "completed",
  "priority": "medium",
  "due_date": "2023-12-31T23:59:59Z"
}
```

#### Headers
```
Authorization: Bearer <valid_jwt_token>
```

#### Responses
- `200 OK`: Task updated successfully
- `400 Bad Request`: Invalid input data
- `401 Unauthorized`: Invalid or expired token
- `404 Not Found`: Task not found or belongs to another user

### DELETE /tasks/{task_id}
Delete a specific task.

#### Path Parameters
- `task_id`: Integer ID of the task to delete

#### Headers
```
Authorization: Bearer <valid_jwt_token>
```

#### Responses
- `204 No Content`: Task deleted successfully
- `401 Unauthorized`: Invalid or expired token
- `404 Not Found`: Task not found or belongs to another user

---

## Health Check Endpoint

### GET /
Health check endpoint to verify the application is running.

#### Responses
- `200 OK`:
  ```json
  {
    "message": "Todo Application API",
    "status": "running"
  }
  ```

---

## Error Codes

| Code | Description |
|------|-------------|
| 400 | Bad Request - Invalid input data |
| 401 | Unauthorized - Missing or invalid authentication token |
| 403 | Forbidden - Insufficient permissions |
| 404 | Not Found - Resource does not exist |
| 409 | Conflict - Resource already exists (e.g., duplicate email) |
| 422 | Unprocessable Entity - Validation error |
| 500 | Internal Server Error - Unexpected server error |

---

## Rate Limiting
The API implements rate limiting to prevent abuse:
- Anonymous users: 100 requests per hour
- Authenticated users: 1000 requests per hour

Rate limit headers are included in all responses:
- `X-RateLimit-Limit`: Maximum requests allowed
- `X-RateLimit-Remaining`: Remaining requests
- `X-RateLimit-Reset`: Time when rate limit resets

---

## Security Best Practices

1. Always use HTTPS in production
2. Validate and sanitize all input data
3. Implement proper authentication and authorization
4. Protect against common attacks (XSS, CSRF, SQL injection)
5. Regularly update dependencies
6. Monitor for suspicious activities
7. Implement proper logging and alerting