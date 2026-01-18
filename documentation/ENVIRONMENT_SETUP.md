# Environment Setup

## Overview
The application now uses a single consolidated `.env` file for all environment variables. This simplifies configuration management and reduces redundancy.

## Environment File
- **File**: `.env` (single consolidated file)
- **Location**: Project root directory
- **Purpose**: Contains all environment-specific configurations for the application

## Required Environment Variables

### Application Configuration
```
APP_NAME=Todo Application API
DEBUG=true
ENVIRONMENT=development
```

### Database Configuration
```
DATABASE_URL=sqlite:///./todo_app.db
NEON_DATABASE_URL=psql 'postgresql://neondb_owner:npg_hAXfM7oN1pqV@ep-empty-recipe-ahl0tehx-pooler.c-3.us-east-1.aws.neon.tech/neondb?sslmode=require&channel_binding=require'
DB_ECHO=false
```

### JWT Configuration
```
SECRET_KEY=Lli0kaoi1N2l4UEpTiwK0WauXOvme2IK
ALGORITHM=HS256
ACCESS_TOKEN_EXPIRE_MINUTES=15
```

### Better Auth Configuration
```
BETTER_AUTH_SECRET=Lli0kaoi1N2l4UEpTiwK0WauXOvme2IK
BETTER_AUTH_URL=http://localhost:3000
```

### Logging Configuration
```
LOG_LEVEL=INFO
LOG_FILE=logs/app.log
```

### CORS Configuration
```
ALLOWED_ORIGINS=*
```

### Frontend Configuration
```
NEXT_PUBLIC_API_BASE_URL=https://ambreen-dev-todo-app.hf.space
```

## Setup Instructions

### 1. Clone the Repository
```bash
git clone <repository-url>
cd todo-web-app
```

### 2. Environment Configuration
The `.env` file is already configured with default values. Update as needed for your environment:

```bash
# Edit the environment file
nano .env
```

### 3. Backend Setup
```bash
# Navigate to backend
cd backend

# Create virtual environment
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt

# Start the application
uvicorn src.main:app --reload
```

### 4. Frontend Setup
```bash
# Navigate to frontend
cd frontend

# Install dependencies
npm install

# Start development server
npm run dev
```

### 5. Docker Setup
```bash
# Build and run with Docker Compose
docker-compose up --build
```

## Environment Modes

### Development
- `DEBUG=true`
- `ENVIRONMENT=development`
- SQLite database by default
- Detailed logging enabled

### Production
- `DEBUG=false`
- `ENVIRONMENT=production`
- PostgreSQL/Neon database
- Minimal logging

## Security Notes
- Never commit the `.env` file to version control
- Use strong, unique values for secrets in production
- Regularly rotate sensitive keys
- Limit access to the environment file

## Troubleshooting

### Missing Environment Variables
If you encounter errors about missing environment variables:
1. Verify the `.env` file exists in the project root
2. Check that all required variables are present
3. Ensure the application has read permissions for the file

### Variable Not Loading
- Ensure the application is started from the project root directory
- Verify the file has the correct name (`.env`)
- Check for syntax errors in the environment file

## Migration Notes
This setup consolidates the previous multiple environment files (`.env.example`, `.env.local`, `.env.production`) into a single `.env` file for simplicity. Update your deployment processes to use the single file instead of multiple environment-specific files.