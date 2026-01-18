#!/bin/bash
# Security Scanning Script for Todo Web App
# This script runs various security scanning tools on the application

set -e  # Exit on any error

echo "Starting security scan for Todo Web App..."

# Create reports directory if it doesn't exist
mkdir -p reports

echo "1. Running Bandit security scan on backend..."
if command -v bandit &> /dev/null; then
    bandit -r backend/src -f json -o reports/backend-bandit-report.json
    echo "Bandit scan completed. Report saved to reports/backend-bandit-report.json"
else
    echo "Bandit not found. Installing..."
    pip install bandit
    bandit -r backend/src -f json -o reports/backend-bandit-report.json
    echo "Bandit scan completed. Report saved to reports/backend-bandit-report.json"
fi

echo "2. Checking for vulnerable dependencies in backend..."
if command -v pip-audit &> /dev/null; then
    pip-audit --requirement backend/requirements.txt --output reports/backend-pip-audit-report.json
    echo "Dependency audit completed. Report saved to reports/backend-pip-audit-report.json"
else
    echo "pip-audit not found. Installing..."
    pip install pip-audit
    pip-audit --requirement backend/requirements.txt --output reports/backend-pip-audit-report.json
    echo "Dependency audit completed. Report saved to reports/backend-pip-audit-report.json"
fi

echo "3. Checking for vulnerable dependencies in frontend..."
if [ -f "frontend/package-lock.json" ]; then
    cd frontend
    if command -v npm &> /dev/null; then
        npm audit --audit-level moderate --json > ../reports/frontend-npm-audit-report.json
        echo "Frontend dependency audit completed. Report saved to reports/frontend-npm-audit-report.json"
    fi
    cd ..
fi

echo "4. Running additional security checks..."
# Add any additional security checks here
# For example, checking for exposed secrets in code
if command -v truffleHog &> /dev/null; then
    truffleHog --regex --entropy=False . > reports/trufflehog-report.txt
    echo "Secrets scan completed. Report saved to reports/trufflehog-report.txt"
elif command -v gitleaks &> /dev/null; then
    gitleaks detect --report-format json --report-path reports/gitleaks-report.json
    echo "Secrets scan completed. Report saved to reports/gitleaks-report.json"
else
    echo "TruffleHog or Gitleaks not found. Skipping secrets scan."
fi

echo "Security scanning completed!"
echo "Reports are available in the 'reports' directory:"
ls -la reports/