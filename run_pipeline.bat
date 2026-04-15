@echo off
title MLB HR Bot - Running Pipeline
cd /d "%~dp0"
call venv\Scripts\activate.bat
echo Running full day pipeline...
python pipeline\workflow.py --stage full
echo.
echo Done! Refresh your browser dashboard.
pause
