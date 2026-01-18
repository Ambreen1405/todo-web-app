# Deployment Documentation

## Overview
This document provides instructions for deploying the Todo Web Application in various environments, including development, staging, and production.

## Prerequisites

### System Requirements
- **Operating System**: Linux, macOS, or Windows with WSL2
- **Docker**: Version 20.10 or higher
- **Docker Compose**: Version 2.0 or higher
- **Git**: Version 2.0 or higher
- **Python**: 3.9+ (for local development)
- **Node.js**: 18+ (for local development)

### Environment Variables
The application requires several environment variables to be set. Create `.env` files in the root directory:

```bash
# .env (for production)
APP_NAME=Todo Application API - Production
DEBUG=false
ENVIRONMENT=production

# Database Configuration
DATABASE_URL=postgresql://username:password@host:port/todo_production_db
NEON_DATABASE_URL=your-production-neon-database-url

# JWT Configuration
SECRET_KEY=your-very-secure-production-key
ALGORITHM=HS256
ACCESS_TOKEN_EXPIRE_MINUTES=30

# CORS Configuration
ALLOWED_ORIGINS=https://yourdomain.com,https://www.yourdomain.com

# Frontend Configuration
NEXT_PUBLIC_API_BASE_URL=https://api.yourdomain.com

# Better Auth Configuration
BETTER_AUTH_SECRET=your-production-better-auth-secret-key
BETTER_AUTH_URL=https://yourdomain.com
```

## Deployment Methods

### 1. Containerized Deployment (Recommended)

#### Quick Start with Docker Compose
```bash
# Clone the repository
git clone <repository-url>
cd todo-web-app

# The .env file is already configured
# Edit .env with your specific values as needed

# Start the application
docker-compose up -d
```

#### Build and Push Images
```bash
# Build and push images to registry
docker-compose build
docker-compose push  # If using a registry
```

#### Deploy with Docker Compose
```bash
# Pull latest images
docker-compose pull

# Start/upgrade services
docker-compose up -d

# View logs
docker-compose logs -f
```

### 2. Manual Deployment

#### Backend Deployment
```bash
# Navigate to backend directory
cd backend

# Create virtual environment
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt

# Run database migrations
alembic upgrade head

# Start the application
uvicorn src.main:app --host 0.0.0.0 --port 8000
```

#### Frontend Deployment
```bash
# Navigate to frontend directory
cd frontend

# Install dependencies
npm install

# Build the application
npm run build

# Start the application
npm start
```

### 3. Cloud Deployment

#### Deploy to AWS
1. Set up an ECS cluster
2. Push Docker images to ECR
3. Configure load balancer and security groups
4. Deploy using the provided docker-compose file

#### Deploy to Azure
1. Create Azure Container Instances or AKS cluster
2. Push Docker images to Azure Container Registry
3. Deploy using ARM templates or Azure CLI

#### Deploy to Google Cloud
1. Set up Google Kubernetes Engine
2. Push Docker images to Google Container Registry
3. Deploy using the provided Kubernetes manifests

## Configuration Management

### Environment-Specific Configurations

#### Development
```bash
# .env.development
DEBUG=true
DATABASE_URL=postgresql://localhost:5432/todo_dev
SECRET_KEY=dev-secret-key
NEXT_PUBLIC_API_BASE_URL=https://ambreen-dev-todo-app.hf.space
```

#### Staging
```bash
# .env.staging
DEBUG=false
DATABASE_URL=postgresql://staging-db:5432/todo_staging
SECRET_KEY=staging-secret-key
NEXT_PUBLIC_API_BASE_URL=https://staging-api.yourdomain.com
```

#### Production
```bash
# .env.production
DEBUG=false
DATABASE_URL=postgresql://prod-db:5432/todo_prod
SECRET_KEY=production-secret-key
NEXT_PUBLIC_API_BASE_URL=https://api.yourdomain.com
```

### Secrets Management
Store sensitive information like database passwords and API keys in your cloud provider's secrets management system:
- AWS Systems Manager Parameter Store
- Azure Key Vault
- Google Secret Manager
- HashiCorp Vault

## Database Management

### Initial Setup
```bash
# Run database migrations
docker-compose exec backend alembic upgrade head
```

### Backup and Restore
```bash
# Backup database
docker-compose exec db pg_dump -U postgres -d todo_app > backup.sql

# Restore database
cat backup.sql | docker-compose exec -T db psql -U postgres -d todo_app
```

### Schema Changes
1. Create a new migration: `alembic revision --autogenerate -m "Description of change"`
2. Review and edit the generated migration file
3. Apply the migration: `alembic upgrade head`

## Monitoring and Logging

### Application Logs
```bash
# View all logs
docker-compose logs -f

# View specific service logs
docker-compose logs -f backend
docker-compose logs -f frontend
```

### Health Checks
The application exposes health check endpoints:
- Backend: `GET /health` (returns health status)

### Metrics Collection
Configure your monitoring system to collect metrics from:
- Application logs in the `logs/` directory
- Docker stats for container performance
- Database performance metrics

## Scaling

### Horizontal Scaling
1. Adjust replica counts in docker-compose file:
```yaml
backend:
  deploy:
    replicas: 3
frontend:
  deploy:
    replicas: 2
```

2. Use a load balancer to distribute traffic
3. Ensure shared storage for session data

### Vertical Scaling
1. Increase container resources:
```yaml
backend:
  deploy:
    resources:
      limits:
        cpus: '0.50'
        memory: 1G
      reservations:
        cpus: '0.25'
        memory: 512M
```

## Security Considerations

### HTTPS Configuration
Configure SSL/TLS termination at:
- Load balancer level
- Reverse proxy (nginx) level
- CDN level

### Firewall Rules
- Allow only necessary ports (80, 443, 22)
- Restrict database access to application containers only
- Use security groups/network ACLs

### Vulnerability Management
- Regularly update base images
- Run security scans in CI/CD pipeline
- Monitor for CVEs in dependencies

## Troubleshooting

### Common Issues

#### Database Connection Issues
```bash
# Check if database is running
docker-compose ps db

# Check database logs
docker-compose logs db

# Test connection from backend
docker-compose exec backend ping db
```

#### Application Startup Issues
```bash
# Check application logs
docker-compose logs backend
docker-compose logs frontend

# Check environment variables
docker-compose exec backend env
```

#### Performance Issues
```bash
# Check resource usage
docker stats

# Check application logs for slow queries
docker-compose logs backend | grep -i "slow"
```

### Diagnostic Commands
```bash
# Check all running containers
docker-compose ps

# Check network connectivity
docker-compose exec backend nslookup db

# Check volume mounts
docker-compose exec backend ls -la /app/logs
```

## Rollback Procedures

### Quick Rollback
```bash
# If using git tags for versions
git checkout <previous-tag>
docker-compose down
docker-compose up -d --build
```

### Database Rollback
```bash
# Rollback to previous migration
docker-compose exec backend alembic downgrade -1
```

## Maintenance Tasks

### Regular Maintenance
- Update dependencies monthly
- Rotate secrets quarterly
- Review and clean logs weekly
- Monitor disk space daily

### Backup Verification
- Test backup restoration monthly
- Verify backup integrity weekly
- Store backups in multiple regions

## Deployment Checklist

### Pre-Deployment
- [ ] Run all tests successfully
- [ ] Verify environment variables are set correctly
- [ ] Check available disk space
- [ ] Ensure backup is current
- [ ] Notify team of deployment window

### During Deployment
- [ ] Monitor application health
- [ ] Check for error spikes
- [ ] Verify functionality manually
- [ ] Monitor resource usage

### Post-Deployment
- [ ] Confirm all services are healthy
- [ ] Run smoke tests
- [ ] Update documentation if needed
- [ ] Communicate deployment success/failure
- [ ] Monitor for issues for at least 30 minutes

## Contact Information
- **Deployment Team**: [team-email@company.com]
- **On-Call Engineer**: [oncall-phone-number]
- **Incident Response**: [incident-response-channel]