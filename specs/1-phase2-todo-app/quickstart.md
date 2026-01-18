# Quickstart Guide: Todo Application Phase 2

## Overview
This guide provides a quick overview of how to set up and run the Todo Application Phase 2 project. The application consists of a Next.js frontend and a FastAPI backend with PostgreSQL database.

## Prerequisites
- Node.js 18+ (for frontend)
- Python 3.11+ (for backend)
- PostgreSQL 12+ (database)
- Git

## Setup Instructions

### 1. Clone the Repository
```bash
git clone <repository-url>
cd <project-directory>
```

### 2. Backend Setup
```bash
cd backend
pip install -r requirements.txt
```

Create a `.env` file in the backend directory:
```env
DATABASE_URL=postgresql://username:password@localhost/todo_app_db
SECRET_KEY=your-super-secret-key-here
ALGORITHM=HS256
ACCESS_TOKEN_EXPIRE_MINUTES=15
```

Initialize the database:
```bash
# Create the database if it doesn't exist
# Run database migrations
alembic upgrade head
```

Start the backend:
```bash
uvicorn main:app --reload
```

### 3. Frontend Setup
```bash
cd frontend
npm install
```

Create a `.env.local` file in the frontend directory:
```env
NEXT_PUBLIC_API_BASE_URL=https://ambreen-dev-todo-app.hf.space
```

Start the frontend:
```bash
npm run dev
```

## API Endpoints

### Authentication
- `POST /auth/register` - Register a new user
- `POST /auth/login` - Login user and get JWT token
- `POST /auth/logout` - Logout user

### Tasks
- `GET /tasks` - Get all tasks for the authenticated user
- `POST /tasks` - Create a new task
- `GET /tasks/{id}` - Get a specific task
- `PUT /tasks/{id}` - Update a task
- `DELETE /tasks/{id}` - Delete a task

## Environment Variables

### Backend (.env)
- `DATABASE_URL` - PostgreSQL database connection string
- `SECRET_KEY` - Secret key for JWT token signing
- `ALGORITHM` - Algorithm for JWT encoding (default: HS256)
- `ACCESS_TOKEN_EXPIRE_MINUTES` - JWT token expiration time

### Frontend (.env.local)
- `NEXT_PUBLIC_API_BASE_URL` - Base URL for API calls

## Development Workflow

1. **Database Changes**: Use Alembic for database migrations
2. **API Changes**: Update the API contract and implement in FastAPI
3. **UI Changes**: Modify components in the Next.js app directory
4. **Authentication**: All protected routes require a valid JWT token in the Authorization header

## Testing

### Backend Tests
```bash
cd backend
pytest
```

### Frontend Tests
```bash
cd frontend
npm run test
```

## Deployment

### Backend
- Set production environment variables
- Run database migrations
- Deploy using your preferred method (Docker, cloud provider, etc.)

### Frontend
- Build for production: `npm run build`
- Serve the static assets using a web server or CDN

## Troubleshooting

### Common Issues
- **Database Connection**: Verify DATABASE_URL is correct and database is running
- **JWT Expiration**: Tokens expire after 15 minutes by default; implement refresh logic
- **CORS Issues**: Configure CORS settings in the FastAPI backend for production

### API Testing
Use the API contract file (contracts/api-contract.yml) to understand request/response formats.