@echo off
title MLB HR Bot - Backtest
cd /d "%~dp0"
call venv\Scripts\activate.bat
echo Running backtest...
python backtest\backtest.py %*
pause
