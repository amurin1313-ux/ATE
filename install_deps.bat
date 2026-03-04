@echo off
setlocal EnableExtensions EnableDelayedExpansion
cd /d %~dp0

REM ============================
REM ATE 6PRO v3.x — deps install
REM ============================

set "PY=python"
where py >nul 2>&1 && set "PY=py -3"

if not exist ".venv\Scripts\python.exe" (
  echo [ATE] Creating venv...
  %PY% -m venv .venv
  if errorlevel 1 goto :fail
)

set "VPY=.venv\Scripts\python.exe"

echo [ATE] Installing deps into venv...
%VPY% -m pip install --upgrade pip
if errorlevel 1 goto :fail
%VPY% -m pip install -r requirements.txt
if errorlevel 1 goto :fail

echo.
echo DONE. Now run: run_app.bat
pause
exit /b 0

:fail
echo.
echo [ATE] INSTALL FAILED. Scroll up to see the error.
pause
exit /b 1
