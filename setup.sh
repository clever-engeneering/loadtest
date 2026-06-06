#!/usr/bin/env bash
# Создаёт виртуальное окружение и ставит зависимости (Linux / macOS).
# Использует самый свежий доступный python3.X (>=3.10).
set -euo pipefail
cd "$(dirname "$0")"

PY=""
for v in python3.13 python3.12 python3.11 python3.10 python3; do
    if command -v "$v" >/dev/null 2>&1; then PY="$v"; break; fi
done
[ -z "$PY" ] && { echo "Не найден python3 (>=3.10). Установите Python."; exit 1; }

echo "Использую: $($PY --version)"
"$PY" -m venv .venv
.venv/bin/python -m pip install --upgrade pip
.venv/bin/pip install -r requirements.txt
echo
echo "Готово. Запуск:"
echo "  .venv/bin/python loadtest.py --url <URL> -c 100 -d 5 -H 'x-api-key: ...'"
