@echo off
title MLB HR Bot
cd /d "%~dp0"

echo.
echo  =============================================
echo    MLB Home Run Bot - Starting up...
echo  =============================================
echo.

REM Check Python
python --version >nul 2>&1
if errorlevel 1 (
    echo ERROR: Python is not installed or not in PATH.
    echo Download Python from https://www.python.org/downloads/
    echo Make sure to check "Add Python to PATH" during install.
    pause
    exit /b
)

REM Create virtual environment if it doesn't exist
if not exist "venv\Scripts\activate.bat" (
    echo Creating Python virtual environment...
    python -m venv venv
)

REM Activate virtual environment
call venv\Scripts\activate.bat

REM Install dependencies if needed
echo Checking dependencies...
pip install -q -r requirements.txt

REM Initialize the database
echo Initializing database...
python -c "from data.schema import init_db; init_db()"

echo.
echo  Dashboard starting at: http://localhost:8000
echo  Open that URL in your browser.
echo  Press Ctrl+C to stop.
echo.

REM Start the dashboard
python -m uvicorn dashboard.app:app --host 127.0.0.1 --port 8000
pause
