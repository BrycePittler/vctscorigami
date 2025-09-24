@echo off
cd /d "C:\Users\Bpitt\Desktop\vctscorigami"
pip install Flask
pip install bcrypt
call .\venv\Scripts\activate
start http://127.0.0.1:5000
python app.py