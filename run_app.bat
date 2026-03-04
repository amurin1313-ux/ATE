@echo off
setlocal
cd /d %~dp0

REM Start the desktop app (uses venv if present)
if exist ".venv\Scripts\python.exe" (
  .venv\Scripts\python.exe -m app.main
) else (
  python -m app.main
)

pause
