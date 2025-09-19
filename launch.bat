@echo off
cd /d "C:\Users\Bpitt\Desktop\vctscorigami"
call .\venv\Scripts\activate
start http://127.0.0.1:5000
python app.py