@echo off
setlocal

REM Always run from this script's folder.
cd /d "%~dp0"

if not exist ".venv\Scripts\python.exe" (
    echo [INFO] .venv not found. Creating virtual environment...
    py -3 -m venv .venv
    if errorlevel 1 (
        echo [ERROR] Failed to create virtual environment.
        exit /b 1
    )
)

echo [INFO] Installing/updating dependencies from requirements.txt...
".venv\Scripts\python.exe" -m pip install --upgrade pip
if errorlevel 1 (
    echo [ERROR] Failed to upgrade pip.
    exit /b 1
)

".venv\Scripts\python.exe" -m pip install -r requirements.txt
if errorlevel 1 (
    echo [ERROR] Failed to install requirements.
    exit /b 1
)

echo [INFO] Starting scanner app...
".venv\Scripts\python.exe" app.py

endlocal
