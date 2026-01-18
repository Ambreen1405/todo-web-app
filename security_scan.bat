@echo off
REM Security Scanning Script for Todo Web App (Windows)
REM This script runs various security scanning tools on the application

echo Starting security scan for Todo Web App...

REM Create reports directory if it doesn't exist
if not exist "reports" mkdir reports

echo 1. Running Bandit security scan on backend...
python -m bandit -r backend/src -f json -o reports\backend-bandit-report.json
if %ERRORLEVEL% EQU 0 (
    echo Bandit scan completed. Report saved to reports\backend-bandit-report.json
) else (
    echo Bandit not found. Installing...
    pip install bandit
    python -m bandit -r backend/src -f json -o reports\backend-bandit-report.json
    echo Bandit scan completed. Report saved to reports\backend-bandit-report.json
)

echo 2. Checking for vulnerable dependencies in backend...
pip-audit --requirement backend\requirements.txt --output reports\backend-pip-audit-report.json
if %ERRORLEVEL% NEQ 0 (
    echo pip-audit not found. Installing...
    pip install pip-audit
    pip-audit --requirement backend\requirements.txt --output reports\backend-pip-audit-report.json
    echo Dependency audit completed. Report saved to reports\backend-pip-audit-report.json
) else (
    echo Dependency audit completed. Report saved to reports\backend-pip-audit-report.json
)

echo 3. Checking for vulnerable dependencies in frontend...
if exist "frontend\package-lock.json" (
    cd frontend
    if exist "package.json" (
        npm audit --audit-level moderate --json > ..\reports\frontend-npm-audit-report.json
        echo Frontend dependency audit completed. Report saved to reports\frontend-npm-audit-report.json
    )
    cd ..
)

echo Security scanning completed!
echo Reports are available in the 'reports' directory:
dir reports