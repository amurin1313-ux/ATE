@echo off
setlocal

REM Replay решений стратегии по Decision Log (offline, без сети)
REM Запускать из корневой папки программы.

python tools\replay_decisions.py --data data

echo.
echo Готово. Отчёт сохранён в папке data (replay_report_*.json)
pause
