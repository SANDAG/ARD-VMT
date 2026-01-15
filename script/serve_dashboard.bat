@ECHO OFF
cd ..\

start "" .venv/Scripts/python.exe -m panel serve ./report/dashboard.py --dev --show
