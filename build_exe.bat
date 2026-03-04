@echo off
setlocal EnableExtensions EnableDelayedExpansion
cd /d %~dp0

REM ============================
REM ATE 6PRO v3.x — EXE build
REM ============================

REM 1) Pick Python launcher
set "PY=python"
where py >nul 2>&1 && set "PY=py -3"

REM 2) Create venv if missing
if not exist ".venv\Scripts\python.exe" (
  echo [ATE] Creating venv...
  %PY% -m venv .venv
  if errorlevel 1 goto :fail
)

set "VPY=.venv\Scripts\python.exe"

REM 3) Upgrade pip + install deps
echo [ATE] Installing deps into venv...
%VPY% -m pip install --upgrade pip
if errorlevel 1 goto :fail
%VPY% -m pip install -r requirements.txt
if errorlevel 1 goto :fail

REM 4) Clean caches + old builds
for /r %%D in (__pycache__) do (
  if exist "%%D" rd /s /q "%%D" >nul 2>&1
)
del /s /q *.pyc >nul 2>&1
if exist build rd /s /q build >nul 2>&1
if exist dist rd /s /q dist >nul 2>&1

REM 5) Build
echo [ATE] Building EXE with PyInstaller...
%VPY% -m PyInstaller --clean ATE_Desktop.spec

if errorlevel 1 goto :fail

echo.
echo DONE. EXE is in dist\ATE_6PRO\ATE_6PRO.exe
echo.
pause
exit /b 0

:fail
echo.
echo [ATE] BUILD FAILED. Scroll up to see the error.
echo.
pause
exit /b 1
