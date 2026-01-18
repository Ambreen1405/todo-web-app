# Backup and Recovery Procedures

## Overview
This document outlines the backup and recovery procedures for the Todo Web Application, covering both the application data and configuration.

## Backup Strategy

### Data Backup
- **Database backups**: Daily full backups with transaction log backups every 4 hours
- **File storage**: Backup of any uploaded files or static assets
- **Configuration**: Backup of application configurations and environment variables

### Backup Schedule
- **Full database backup**: Daily at 2:00 AM UTC
- **Incremental database backup**: Every 4 hours during business hours
- **Configuration backup**: On every deployment
- **Weekly system backup**: Sunday at 1:00 AM UTC

### Backup Retention
- **Daily backups**: Keep for 30 days
- **Weekly backups**: Keep for 3 months
- **Monthly backups**: Keep for 1 year
- **Critical backups**: Keep indefinitely

## Database Backup Procedures

### Automated Backup Script
```bash
#!/bin/bash
# backup-db.sh

DATE=$(date +%Y%m%d_%H%M%S)
BACKUP_DIR="/backups/db"
DB_NAME="todo_app_db"
DB_USER="backup_user"
DB_HOST="localhost"
DB_PORT="5432"

# Create backup directory if it doesn't exist
mkdir -p $BACKUP_DIR

# Create database dump
pg_dump -h $DB_HOST -p $DB_PORT -U $DB_USER -d $DB_NAME -Fc > "$BACKUP_DIR/${DB_NAME}_${DATE}.dump"

# Compress the backup
gzip "$BACKUP_DIR/${DB_NAME}_${DATE}.dump"

# Remove backups older than retention period (30 days)
find $BACKUP_DIR -name "*.dump.gz" -mtime +30 -delete

echo "Database backup completed: ${DB_NAME}_${DATE}.dump.gz"
```

### Environment Variables for Backup
Add to your backup environment:
```
DB_BACKUP_USER=backup_user
DB_BACKUP_PASSWORD=secure_backup_password
BACKUP_STORAGE_PATH=/backups
BACKUP_RETENTION_DAYS=30
ENCRYPTION_KEY=your_encryption_key_for_backups
```

## Application Configuration Backup

### Configuration Files to Backup
- `.env` files (excluding sensitive values)
- `docker-config/docker-compose.yml`
- `nginx.conf`
- SSL certificates
- Application settings

### Backup Script for Configurations
```bash
#!/bin/bash
# backup-config.sh

DATE=$(date +%Y%m%d_%H%M%S)
BACKUP_DIR="/backups/config"
CONFIG_DIRS=(
    "/app/config"
    "/etc/nginx"
    "/etc/ssl/certs"
)

mkdir -p $BACKUP_DIR

# Create archive of configuration directories
tar -czf "$BACKUP_DIR/config_${DATE}.tar.gz" -C / ${CONFIG_DIRS[@]} 2>/dev/null

# Remove old configuration backups
find $BACKUP_DIR -name "config_*.tar.gz" -mtime +30 -delete

echo "Configuration backup completed: config_${DATE}.tar.gz"
```

## Recovery Procedures

### Database Recovery

#### From Full Backup
```bash
#!/bin/bash
# restore-db-full.sh

BACKUP_FILE=$1  # Path to backup file
DB_NAME=$2      # Target database name
DB_USER=$3      # Database user with restore privileges

# Stop application services
sudo systemctl stop todo-app

# Drop and recreate database
psql -U postgres -c "DROP DATABASE IF EXISTS $DB_NAME;"
psql -U postgres -c "CREATE DATABASE $DB_NAME;"

# Restore from backup
pg_restore -h localhost -U $DB_USER -d $DB_NAME -v $BACKUP_FILE

# Restart application services
sudo systemctl start todo-app

echo "Database recovery completed from $BACKUP_FILE"
```

#### From Incremental Backup
```bash
# Apply transaction logs after restoring from full backup
pg_xlogdump /path/to/xlog/file | psql -d restored_db
```

### Application Recovery

#### From Configuration Backup
```bash
#!/bin/bash
# restore-config.sh

BACKUP_FILE=$1  # Path to configuration backup
RESTORE_DIR="/tmp/restore"

# Extract configuration backup
mkdir -p $RESTORE_DIR
tar -xzf $BACKUP_FILE -C $RESTORE_DIR

# Copy configurations to appropriate locations
sudo cp -r $RESTORE_DIR/app/config/* /app/config/
sudo cp -r $RESTORE_DIR/etc/nginx/* /etc/nginx/
sudo cp -r $RESTORE_DIR/etc/ssl/certs/* /etc/ssl/certs/

# Reload configurations
sudo nginx -t && sudo systemctl reload nginx
sudo systemctl restart todo-app

echo "Configuration recovery completed from $BACKUP_FILE"
```

## Disaster Recovery Plan

### RTO and RPO Targets
- **Recovery Time Objective (RTO)**: 4 hours for critical systems
- **Recovery Point Objective (RPO)**: 4 hours of data loss acceptable

### Recovery Steps
1. **Assessment Phase (0-30 min)**
   - Determine the scope of the disaster
   - Identify affected systems and data
   - Activate disaster recovery team

2. **Recovery Phase (30 min - 4 hours)**
   - Restore from the most recent backup
   - Verify data integrity
   - Bring up minimal viable service
   - Test critical functionality

3. **Restoration Phase (4+ hours)**
   - Restore full service capability
   - Verify all functionality
   - Communicate with stakeholders
   - Document lessons learned

### Recovery Testing
- **Quarterly recovery drills**: Test full recovery procedure
- **Monthly partial recovery**: Test specific components
- **Annual disaster simulation**: Comprehensive disaster scenario

## Backup Monitoring and Alerts

### Backup Success/Failure Monitoring
- Log backup operations with timestamps
- Send alerts on backup failures
- Monitor backup storage space
- Verify backup integrity periodically

### Sample Monitoring Script
```bash
#!/bin/bash
# monitor-backups.sh

BACKUP_LOG="/var/log/backup-monitor.log"
ALERT_EMAIL="admin@yourdomain.com"

# Check if backup ran successfully in the last 24 hours
if [ $(find /backups -name "*.dump.gz" -mtime -1 | wc -l) -eq 0 ]; then
    echo "$(date): ERROR - No backups found in the last 24 hours!" >> $BACKUP_LOG
    # Send alert email
    echo "Database backup failed or did not run in the last 24 hours" | mail -s "Backup Alert" $ALERT_EMAIL
else
    echo "$(date): Backup check passed" >> $BACKUP_LOG
fi

# Check available disk space for backups
AVAILABLE_SPACE=$(df /backups | awk 'NR==2 {print $4}')
MIN_SPACE_GB=10

if [ $((AVAILABLE_SPACE / 1024 / 1024)) -lt $MIN_SPACE_GB ]; then
    echo "$(date): WARNING - Low disk space for backups: ${AVAILABLE_SPACE}KB" >> $BACKUP_LOG
    echo "Low disk space for backups: ${AVAILABLE_SPACE}KB available" | mail -s "Disk Space Alert" $ALERT_EMAIL
fi
```

## Security Considerations

### Encrypted Backups
- Encrypt backup files using AES-256 encryption
- Store encryption keys separately from backups
- Use different keys for different backup types

### Access Control
- Restrict access to backup files to authorized personnel only
- Audit access to backup files
- Implement role-based access control for backup operations

### Compliance
- Ensure backups comply with data protection regulations (GDPR, CCPA)
- Maintain proper data retention policies
- Securely dispose of expired backups

## Contact Information
- **Primary DR Contact**: [Name, Phone, Email]
- **Secondary DR Contact**: [Name, Phone, Email]
- **Database Administrator**: [Name, Phone, Email]
- **System Administrator**: [Name, Phone, Email]

## Emergency Contacts
- **On-Call Engineer**: [Phone Number]
- **Management Escalation**: [Phone Number]
- **Vendor Support**: [Support Phone Number]