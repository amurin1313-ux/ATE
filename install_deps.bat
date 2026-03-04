@echo off
setlocal EnableExtensions
cd /d %~dp0

set "PY=python"
where py >nul 2>&1 && set "PY=py -3"

if not exist ".venv\Scripts\python.exe" (
  echo [ATE] Creating venv...
  %PY% -m venv .venv || goto :fail
)

set "VPY=.venv\Scripts\python.exe"
%VPY% -m pip install --upgrade pip || goto :fail
%VPY% -m pip install -r requirements.txt || goto :fail

echo [ATE] Dependencies installed.
exit /b 0

:fail
echo [ATE] INSTALL FAILED
exit /b 1
