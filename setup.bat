@echo off
REM Создаёт виртуальное окружение и ставит зависимости (Windows).
setlocal
cd /d "%~dp0"

set "PY="
for %%v in (py python) do (
    where %%v >nul 2>nul && set "PY=%%v"
)
if "%PY%"=="" (
    echo Не найден Python. Установите Python 3.10+ с python.org
    exit /b 1
)

echo Использую:
%PY% --version
%PY% -m venv .venv
.venv\Scripts\python.exe -m pip install --upgrade pip
.venv\Scripts\pip.exe install -r requirements.txt
echo.
echo Готово. Запуск:
echo   .venv\Scripts\python.exe loadtest.py --url ^<URL^> -c 100 -d 5 -H "x-api-key: ..."
endlocal
