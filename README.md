# Todo Application Phase 2

A secure, full-stack web application with Next.js frontend, FastAPI backend, PostgreSQL database, and JWT-based authentication.

## Overview

This application allows users to register accounts, log in securely, and manage their personal tasks. The system enforces data isolation so users can only access their own tasks. The application features a modern UI with responsive design, advanced task management capabilities, and comprehensive security measures.

## Architecture

- **Frontend**: Next.js (App Router) with TypeScript and Tailwind CSS
- **Backend**: FastAPI with Python 3.11
- **Database**: PostgreSQL with SQLModel ORM
- **Authentication**: JWT-based authentication with secure token handling

## Project Structure

```
├── backend/
│   ├── src/
│   │   ├── models/
│   │   ├── services/
│   │   ├── api/
│   │   └── database/
│   ├── requirements.txt
│   └── alembic/
├── frontend/
│   ├── src/
│   │   ├── app/
│   │   │   ├── api/
│   │   │   ├── components/
│   │   │   ├── pages/
│   │   │   ├── context/
│   │   │   ├── types/
│   │   │   └── utils/
│   ├── package.json
│   └── next.config.js
├── .env
└── README.md
```

## Setup

### Backend Setup

1. Navigate to the backend directory: `cd backend`
2. Install dependencies: `pip install -r requirements.txt`
3. Configure environment variables in `.env` file
4. Initialize the database: `alembic upgrade head`
5. Start the server: `uvicorn src.main:app --reload`

### Frontend Setup

1. Navigate to the frontend directory: `cd frontend`
2. Install dependencies: `npm install`
3. Configure environment variables in `.env` file as needed
4. Start the development server: `npm run dev`

## Features

### User Management
- User registration with email validation
- Secure login/logout functionality
- JWT-based session management
- User data isolation and privacy

### Task Management
- **Create Tasks**: Add new tasks with title, description, priority, due date, category, and recurrence options
- **Read Tasks**: View all tasks with filtering and sorting capabilities
- **Update Tasks**: Modify task details including title, description, completion status, priority, etc.
- **Delete Tasks**: Remove tasks with confirmation modal
- **Mark Complete/Incomplete**: Toggle task completion status

### Advanced Task Features
- **Task Properties**:
  - Title (automatically capitalizes first letter)
  - Description (optional)
  - Priority levels: Low, Medium, High
  - Due dates with calendar picker
  - Categories: Work, Home, Other
  - Recurrence options: None, Daily, Weekly, Monthly
- **Smart Filtering**: Filter by all, active, completed, overdue, or upcoming tasks
- **Advanced Sorting**: Sort by creation date, due date, priority, or title
- **Search Functionality**: Search through task titles and descriptions

### Dashboard & Analytics
- **Task Statistics**: Real-time counters for total, completed, and pending tasks
- **Quick Actions**: Direct links to view tasks or create new ones
- **Recent Activity Feed**: Track task-related activities (creation, updates, completion, deletion)

### User Interface
- **Modern Design**: Purple-to-pink gradient theme with clean, contemporary UI
- **Responsive Layout**: Fully responsive design that works on mobile, tablet, and desktop
- **Intuitive Navigation**: Easy-to-use navigation with consistent layout across pages
- **Loading States**: Visual feedback during API calls and data loading
- **Error Handling**: Clear error messages and validation feedback

### Security Features
- Passwords are securely hashed using bcrypt
- JWT tokens with proper expiration times
- Input validation and sanitization (XSS protection)
- Data isolation - users can only access their own tasks
- Protection against common web vulnerabilities
- Secure API endpoints with proper authentication checks

### Technical Features
- **API Layer**: Centralized API client using native fetch
- **State Management**: Context API for authentication and loading states
- **Type Safety**: Full TypeScript support with type definitions
- **Component Architecture**: Reusable, modular components
- **Real-time Updates**: Event-driven updates across components
- **Activity Tracking**: Comprehensive logging of user actions

## Environment Variables

### Frontend (.env)
- `NEXT_PUBLIC_API_BASE_URL`: Backend API URL (default: https://ambreen-dev-todo-app.hf.space)
- `NEXT_PUBLIC_APP_URL`: Frontend application URL

### Backend (.env)
- `DATABASE_URL`: PostgreSQL database connection string
- `SECRET_KEY`: JWT secret key for token signing
- `ALGORITHM`: JWT algorithm (default: HS256)
- `ACCESS_TOKEN_EXPIRE_MINUTES`: Token expiration time in minutes

## API Endpoints

### Authentication
- `POST /auth/register` - User registration
- `POST /auth/login` - User login
- `POST /auth/logout` - User logout

### Tasks
- `GET /tasks` - Get all user tasks
- `POST /tasks` - Create a new task
- `GET /tasks/{id}` - Get a specific task
- `PUT /tasks/{id}` - Update a task
- `DELETE /tasks/{id}` - Delete a task

## Contributing

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add some amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

## Testing

The application includes comprehensive test suites for both frontend and backend components. Run tests using the provided test scripts in each respective directory.

## Deployment

For production deployment:
1. Set up environment variables for production
2. Build the frontend: `npm run build`
3. Deploy the backend with proper security configurations
4. Configure reverse proxy and SSL certificates
5. Set up database backups and monitoring

## License

This project is licensed under the MIT License - see the LICENSE file for details.