#!/usr/bin/env python3
"""
Локальный HTTP-сервер для проверки loadtest.py.

Асинхронный (asyncio), без внешних зависимостей — один процесс, один цикл событий,
держит десятки тысяч одновременных keep-alive соединений (до ~65000) без потоков.
Умеет имитировать поведение бэкенда: задержку ответа, долю ошибок и проверку
API-ключа.

Запуск:
    python3 testserver.py                 # 0.0.0.0:7777
    python3 testserver.py --port 8080
    python3 testserver.py --delay 0.05    # +50 мс к каждому ответу
    python3 testserver.py --error-rate 0.1  # 10% ответов с кодом 503
    python3 testserver.py --api-key bee03ffd-3c6f-4e88-8b7b-b4ac673c9cec  # требовать ключ

Под большую нагрузку (тысячи соединений) поднимите лимит дескрипторов:
    ulimit -n 200000        # Linux / macOS, в текущей сессии

Проверка вручную:
    curl -X GET 'http://127.0.0.1:7777/users' -H 'accept: application/json'

Нагрузочный тест против него:
    .venv/bin/python loadtest.py --url http://127.0.0.1:7777/users -c 1000 -d 1
"""

import argparse
import asyncio
import json
import random
import time

# Заполняется из аргументов командной строки в main().
CONFIG = {"delay": 0.0, "error_rate": 0.0, "api_key": None}

# Метрики (один event loop — обычные int безопасны).
_total = 0       # всего обработано запросов
_active = 0       # активных соединений прямо сейчас

# Заранее заготовленные тела/ответы, чтобы не собирать их на каждый запрос.
_BODY_401 = json.dumps({"error": "invalid or missing x-api-key"}).encode()
_BODY_503 = json.dumps({"error": "service unavailable (simulated)"}).encode()


def raise_fd_limit(target: int = 200000) -> int:
    """Поднять мягкий лимит открытых файлов (сокетов) до жёсткого/целевого.

    Без этого при тысячах соединений возникает OSError [Errno 24] Too many open
    files. На Windows модуля resource нет — там лимит и так высокий, просто выходим.
    Возвращает действующий мягкий лимит (или -1, если узнать не удалось).
    """
    try:
        import resource
    except ImportError:
        return -1  # Windows
    soft, hard = resource.getrlimit(resource.RLIMIT_NOFILE)
    # hard может быть RLIM_INFINITY — тогда ориентируемся на target.
    desired = target if hard == resource.RLIM_INFINITY else min(target, hard)
    if desired > soft:
        try:
            resource.setrlimit(resource.RLIMIT_NOFILE, (desired, hard))
            soft = desired
        except (ValueError, OSError):
            # Не дали поднять до desired — пробуем максимум, что разрешён.
            try:
                resource.setrlimit(resource.RLIMIT_NOFILE, (hard, hard))
                soft = hard
            except (ValueError, OSError):
                pass
    return soft


def _response(status: int, reason: str, body: bytes, keep_alive: bool) -> bytes:
    conn = "keep-alive" if keep_alive else "close"
    head = (
        f"HTTP/1.1 {status} {reason}\r\n"
        f"Content-Type: application/json\r\n"
        f"Content-Length: {len(body)}\r\n"
        f"Connection: {conn}\r\n"
        f"\r\n"
    ).encode()
    return head + body


async def handle(reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
    """Обслуживает одно TCP-соединение в режиме keep-alive."""
    global _total, _active
    _active += 1
    try:
        while True:
            # 1. Читаем строку запроса и заголовки до пустой строки.
            try:
                raw = await reader.readuntil(b"\r\n\r\n")
            except (asyncio.IncompleteReadError, ConnectionResetError):
                return  # клиент закрыл соединение
            except asyncio.LimitOverrunError:
                return  # слишком длинные заголовки — закрываем

            lines = raw.split(b"\r\n")
            if not lines or not lines[0]:
                return
            request_line = lines[0].decode("latin-1")

            headers = {}
            for line in lines[1:]:
                if not line or b":" not in line:
                    continue
                k, _, v = line.partition(b":")
                headers[k.decode("latin-1").strip().lower()] = v.decode("latin-1").strip()

            # 2. Если есть тело (Content-Length) — вычитываем его.
            try:
                length = int(headers.get("content-length", "0"))
            except ValueError:
                length = 0
            if length > 0:
                try:
                    await reader.readexactly(length)
                except (asyncio.IncompleteReadError, ConnectionResetError):
                    return

            keep_alive = headers.get("connection", "keep-alive").lower() != "close"
            _total += 1

            # 3. Имитация задержки бэкенда.
            if CONFIG["delay"] > 0:
                await asyncio.sleep(CONFIG["delay"])

            # 4. Проверка API-ключа.
            if CONFIG["api_key"] is not None and headers.get("x-api-key") != CONFIG["api_key"]:
                writer.write(_response(401, "Unauthorized", _BODY_401, keep_alive))
            # 5. Имитация случайных ошибок.
            elif CONFIG["error_rate"] > 0 and random.random() < CONFIG["error_rate"]:
                writer.write(_response(503, "Service Unavailable", _BODY_503, keep_alive))
            # 6. Успешный ответ.
            else:
                path = request_line.split(" ")[1] if " " in request_line else "/"
                body = json.dumps({"ok": True, "path": path, "request": _total}).encode()
                writer.write(_response(200, "OK", body, keep_alive))

            try:
                await writer.drain()
            except (ConnectionResetError, BrokenPipeError):
                return

            if not keep_alive:
                return
    finally:
        _active -= 1
        try:
            writer.close()
        except Exception:
            pass


async def stats_printer(interval: float = 1.0):
    """Раз в секунду печатает активные соединения, всего запросов и RPS."""
    last = 0
    start = time.perf_counter()
    while True:
        await asyncio.sleep(interval)
        cur = _total
        rps = int((cur - last) / interval)
        last = cur
        elapsed = time.perf_counter() - start
        print(
            f"\r[{elapsed:6.1f}s] соединений: {_active:>6} | "
            f"обработано: {cur:>10} | RPS: {rps:>8}",
            end="", flush=True,
        )


async def run(args):
    CONFIG["delay"] = args.delay
    CONFIG["error_rate"] = args.error_rate
    CONFIG["api_key"] = args.api_key

    fd_limit = raise_fd_limit()

    server = await asyncio.start_server(
        handle, args.host, args.port, backlog=args.backlog
    )

    print("=" * 70)
    print(f"  Тестовый сервер (asyncio): http://{args.host}:{args.port}")
    print(f"  Backlog (очередь приёма) : {args.backlog}")
    print(f"  Лимит дескрипторов (fd)  : {fd_limit if fd_limit > 0 else 'n/a'}")
    print(f"  Задержка ответа          : {args.delay}s")
    print(f"  Доля ошибок (503)        : {args.error_rate * 100:.0f}%")
    print(f"  Проверка x-api-key       : {'да' if args.api_key else 'нет'}")
    print(f"  Пример                   : curl http://127.0.0.1:{args.port}/users")
    print("  Остановка                : Ctrl+C")
    print("=" * 70)

    asyncio.create_task(stats_printer())
    async with server:
        await server.serve_forever()


def main():
    p = argparse.ArgumentParser(
        description="Локальный асинхронный тестовый HTTP-сервер для loadtest.py.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    p.add_argument("--host", default="0.0.0.0", help="Адрес для прослушивания")
    p.add_argument("--port", type=int, default=7777, help="Порт")
    p.add_argument("--delay", type=float, default=0.0,
                   help="Искусственная задержка ответа, секунд")
    p.add_argument("--error-rate", type=float, default=0.0,
                   help="Доля ответов с ошибкой 503 (0.0–1.0)")
    p.add_argument("--api-key", default=None,
                   help="Если задан — требовать заголовок x-api-key с этим значением")
    p.add_argument("--backlog", type=int, default=65535,
                   help="Размер очереди принимаемых соединений")
    args = p.parse_args()

    try:
        asyncio.run(run(args))
    except KeyboardInterrupt:
        print("\nОстановка сервера.")


if __name__ == "__main__":
    main()
