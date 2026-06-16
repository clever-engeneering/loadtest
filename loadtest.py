#!/usr/bin/env python3
"""
Скрипт нагрузочного тестирования бэкенда.

Динамически наращивает количество параллельных соединений (от 0 до --connections),
в течение заданного времени шлёт запросы, обрабатывает ответы и в реальном времени
показывает: число установленных соединений, успешные и неуспешные ответы.

Зависимости:
    pip install aiohttp

Пример запуска:
    python3 loadtest.py \
        --url http://10.0.8.49:7777/users \
        --connections 100 \
        --duration 5 \
        --header "x-client-id: 981824de-449d-482a-8578-cba625f7f57c" \
        --header "x-api-key: bee03ffd-3c6f-4e88-8b7b-b4ac673c9cec"
"""

from __future__ import annotations

import argparse
import asyncio
import os
import random
import signal
import sys
import time
from dataclasses import dataclass, field
from datetime import datetime

try:
    import aiohttp
except ImportError:
    sys.exit("Не установлен aiohttp. Установите: pip install aiohttp")

# Сколько значений задержки держать в выборке для оценки перцентилей.
# Reservoir sampling ограничивает память при миллионах запросов.
LATENCY_RESERVOIR_CAP = 50000

# Сколько секунд ждать штатного завершения воркеров при остановке, прежде чем
# принудительно их отменить (на каждом этапе: штатно, затем после cancel).
GRACEFUL_STOP_SECONDS = 5.0

# Папка, куда по умолчанию складываются PDF-отчёты.
REPORTS_DIR = "reports"


def raise_fd_limit(target: int = 200000) -> int:
    """Поднять мягкий лимит открытых файлов (сокетов) до жёсткого/целевого.

    Каждое соединение — это сокет (дескриптор). Без этого при тысячах соединений
    возникает OSError [Errno 24] Too many open files. На Windows модуля resource
    нет — там лимит и так высокий, просто выходим. Возвращает действующий мягкий
    лимит (или -1, если узнать не удалось).
    """
    try:
        import resource
    except ImportError:
        return -1  # Windows
    soft, hard = resource.getrlimit(resource.RLIMIT_NOFILE)
    desired = target if hard == resource.RLIM_INFINITY else min(target, hard)
    if desired > soft:
        try:
            resource.setrlimit(resource.RLIMIT_NOFILE, (desired, hard))
            soft = desired
        except (ValueError, OSError):
            try:
                resource.setrlimit(resource.RLIMIT_NOFILE, (hard, hard))
                soft = hard
            except (ValueError, OSError):
                pass
    return soft


def ephemeral_port_count() -> int:
    """Сколько локальных портов доступно для исходящих соединений.

    Это жёсткий потолок числа одновременных соединений с ОДНОГО хоста к ОДНОМУ
    адресу:порту назначения — каждому соединению нужен уникальный порт-источник.
    Возвращает оценку или -1, если определить не удалось.
    """
    # Linux
    try:
        with open("/proc/sys/net/ipv4/ip_local_port_range") as f:
            lo, hi = (int(x) for x in f.read().split())
            return hi - lo + 1
    except OSError:
        pass
    # macOS / BSD
    try:
        import subprocess
        out = subprocess.run(
            ["sysctl", "-n", "net.inet.ip.portrange.first", "net.inet.ip.portrange.last"],
            capture_output=True, text=True, timeout=2,
        ).stdout.split()
        if len(out) == 2:
            return int(out[1]) - int(out[0]) + 1
    except Exception:
        pass
    return -1


def warn_about_scale(connections: int) -> None:
    """Предупредить, если число соединений нереалистично для одного хоста/эндпоинта."""
    ports = ephemeral_port_count()
    if 0 < ports <= connections:
        print(
            f"  ВНИМАНИЕ: запрошено {connections} соединений, но к одному адресу:порту\n"
            f"           с этого хоста доступно лишь ~{ports} портов-источников.\n"
            f"           Выше этого потолка соединения не открываются (массовые ERR).\n"
            f"           Расширьте диапазон портов или используйте несколько хостов —\n"
            f"           см. USAGE.md, раздел про пределы нагрузки.",
            file=sys.stderr,
        )
    if connections > 10000:
        print(
            f"  ВНИМАНИЕ: {connections} соединений в одном процессе asyncio — это очень\n"
            f"           много. Один event loop и одно ядро CPU не прокачают такую\n"
            f"           нагрузку: растут задержки и таймауты. Реалистично — сотни-тысячи\n"
            f"           соединений на хост; для большего масштабируйтесь по машинам.",
            file=sys.stderr,
        )


@dataclass
class Stats:
    """Потокобезопасная (в рамках одного event loop) копилка метрик."""
    active: int = 0            # текущее число живых воркеров (соединений)
    success: int = 0           # ответы с кодом 2xx
    failed: int = 0            # ответы 4xx/5xx
    errors: int = 0            # сетевые ошибки / таймауты (ответ не получен)
    total_latency: float = 0.0
    latency_count: int = 0
    min_latency: float = float("inf")
    max_latency: float = 0.0
    status_codes: dict = field(default_factory=dict)
    latencies: list = field(default_factory=list)  # выборка задержек (сек) для перцентилей
    history: list = field(default_factory=list)     # временной ряд для графиков отчёта

    def record_latency(self, sec: float):
        """Учесть задержку одного ответа (с reservoir-выборкой для перцентилей)."""
        self.total_latency += sec
        self.latency_count += 1
        if sec < self.min_latency:
            self.min_latency = sec
        if sec > self.max_latency:
            self.max_latency = sec
        if len(self.latencies) < LATENCY_RESERVOIR_CAP:
            self.latencies.append(sec)
        else:
            j = random.randint(0, self.latency_count - 1)
            if j < LATENCY_RESERVOIR_CAP:
                self.latencies[j] = sec

    @property
    def total(self) -> int:
        return self.success + self.failed + self.errors

    @property
    def avg_latency_ms(self) -> float:
        return (self.total_latency / self.latency_count * 1000) if self.latency_count else 0.0

    def percentiles_ms(self) -> dict:
        """Оценка перцентилей задержки (мс) по выборке."""
        if not self.latencies:
            return {}
        s = sorted(self.latencies)
        n = len(s)

        def pct(p):
            idx = min(n - 1, max(0, int(round(p / 100 * (n - 1)))))
            return s[idx] * 1000

        return {"p50": pct(50), "p90": pct(90), "p95": pct(95), "p99": pct(99)}


async def worker(
    session: aiohttp.ClientSession,
    url: str,
    method: str,
    headers: dict,
    body: str | None,
    timeout: float,
    stop_event: asyncio.Event,
    stats: Stats,
):
    """Один «коннект»: в цикле шлёт запросы, пока не получен сигнал остановки."""
    stats.active += 1
    try:
        while not stop_event.is_set():
            start = time.perf_counter()
            try:
                async with session.request(
                    method,
                    url,
                    headers=headers,
                    data=body,
                    timeout=aiohttp.ClientTimeout(total=timeout),
                ) as resp:
                    await resp.read()  # вычитываем тело, чтобы корректно переиспользовать соединение
                    elapsed = time.perf_counter() - start
                    stats.record_latency(elapsed)
                    stats.status_codes[resp.status] = stats.status_codes.get(resp.status, 0) + 1
                    if 200 <= resp.status < 400:
                        stats.success += 1
                    else:
                        stats.failed += 1
            except asyncio.CancelledError:
                raise
            except Exception:
                stats.errors += 1
    finally:
        stats.active -= 1


async def reporter(stop_event: asyncio.Event, stats: Stats, start_ts: float, duration: float):
    """Раз в секунду печатает текущее состояние и пишет точку временного ряда."""
    last_total = 0
    last_t = 0.0
    while not stop_event.is_set():
        elapsed = time.perf_counter() - start_ts
        rps_avg = stats.total / elapsed if elapsed > 0 else 0
        dt = elapsed - last_t
        rps_interval = (stats.total - last_total) / dt if dt > 0 else 0
        last_total, last_t = stats.total, elapsed
        remaining = max(0.0, duration - elapsed)

        # Точка временного ряда для графиков в PDF-отчёте.
        stats.history.append({
            "t": elapsed,
            "active": stats.active,
            "success": stats.success,
            "failed": stats.failed,
            "errors": stats.errors,
            "rps": rps_interval,
            "avg_ms": stats.avg_latency_ms,
        })

        line = (
            f"\r[{elapsed:6.1f}s | осталось {remaining:6.1f}s] "
            f"conn: {stats.active:>4} | "
            f"OK: {stats.success:>7} | "
            f"FAIL: {stats.failed:>6} | "
            f"ERR: {stats.errors:>6} | "
            f"RPS: {rps_interval:7.1f} | "
            f"avg: {stats.avg_latency_ms:6.1f}ms"
        )
        sys.stdout.write(line)
        sys.stdout.flush()
        try:
            await asyncio.wait_for(stop_event.wait(), timeout=1.0)
        except asyncio.TimeoutError:
            pass


async def ramp_up(
    session, url, method, headers, body, timeout,
    connections: int, ramp_seconds: float,
    stop_event: asyncio.Event, stats: Stats, worker_tasks: list,
):
    """Постепенно (от 0 до connections) поднимает воркеров за ramp_seconds.

    Запускается как фоновая задача параллельно с таймером длительности, поэтому
    созданные воркеры складываются в общий список worker_tasks (чтобы их можно
    было корректно остановить). Моменты запуска считаются от абсолютного времени
    старта, а не накопительными sleep — иначе под нагрузкой паузы «уплывают» и
    ramp растягивается далеко за ramp_seconds.
    """
    start = time.perf_counter()
    for i in range(connections):
        if stop_event.is_set():
            break
        worker_tasks.append(asyncio.create_task(
            worker(session, url, method, headers, body, timeout, stop_event, stats)
        ))
        if ramp_seconds > 0 and i + 1 < connections:
            # Ждём до планового момента запуска следующего воркера.
            target = start + ramp_seconds * (i + 1) / connections
            sleep_for = target - time.perf_counter()
            if sleep_for > 0:
                try:
                    await asyncio.wait_for(stop_event.wait(), timeout=sleep_for)
                    break  # пришёл сигнал остановки во время ramp-up
                except asyncio.TimeoutError:
                    pass


def parse_headers(raw_headers: list[str]) -> dict:
    headers = {"accept": "application/json"}
    for h in raw_headers or []:
        if ":" not in h:
            sys.exit(f"Некорректный заголовок (ожидается 'Key: Value'): {h}")
        key, _, value = h.partition(":")
        headers[key.strip()] = value.strip()
    return headers


async def run(args):
    headers = parse_headers(args.header)
    duration = args.duration * 60.0
    started_at = datetime.now()
    stop_event = asyncio.Event()

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, stop_event.set)
        except NotImplementedError:
            pass  # Windows

    fd_limit = raise_fd_limit()
    if 0 < fd_limit <= args.connections + 50:
        print(f"  ВНИМАНИЕ: лимит дескрипторов ({fd_limit}) близок к числу соединений "
              f"({args.connections}). Поднимите вручную: ulimit -n 200000", file=sys.stderr)
    warn_about_scale(args.connections)

    stats = Stats()
    # Лимит пула соединений = числу воркеров, чтобы реально держать столько коннектов.
    connector = aiohttp.TCPConnector(limit=args.connections, ssl=False)

    print("=" * 80)
    print(f"  URL         : {args.method} {args.url}")
    print(f"  Connections : {args.connections} (ramp-up {args.ramp}s)")
    print(f"  Duration    : {args.duration} мин ({duration:.0f}s)")
    print(f"  Timeout     : {args.timeout}s/запрос")
    print(f"  FD limit    : {fd_limit if fd_limit > 0 else 'n/a'}")
    print("=" * 80)

    async with aiohttp.ClientSession(connector=connector) as session:
        start_ts = time.perf_counter()
        rep = asyncio.create_task(reporter(stop_event, stats, start_ts, duration))

        # Ramp-up идёт параллельно: таймер длительности отсчитывается от старта
        # независимо от того, сколько реально занимает наращивание соединений.
        worker_tasks: list = []
        ramp_task = asyncio.create_task(ramp_up(
            session, args.url, args.method, headers, args.body, args.timeout,
            args.connections, args.ramp, stop_event, stats, worker_tasks,
        ))

        # Ждём заданное время либо сигнал остановки.
        try:
            await asyncio.wait_for(stop_event.wait(), timeout=duration)
        except asyncio.TimeoutError:
            pass

        # 1. Останавливаем наращивание (чтобы worker_tasks больше не пополнялся).
        stop_event.set()
        ramp_task.cancel()
        await asyncio.gather(ramp_task, return_exceptions=True)

        # 2. Останавливаем воркеров. Воркер, «застрявший» внутри запроса (ждёт
        #    ответа до --timeout), не заметит stop_event, пока запрос не завершится.
        #    Поэтому даём короткую паузу на штатное завершение, а затем
        #    ПРИНУДИТЕЛЬНО отменяем — иначе при медленном бэкенде остановка
        #    растягивается на минуты (вплоть до --timeout на каждый «висящий» запрос).
        if worker_tasks:
            sys.stdout.write("\nОстановка: завершаю активные запросы...\n")
            sys.stdout.flush()
            _, pending = await asyncio.wait(worker_tasks, timeout=GRACEFUL_STOP_SECONDS)
            for t in pending:
                t.cancel()
            try:
                await asyncio.wait_for(
                    asyncio.gather(*worker_tasks, return_exceptions=True),
                    timeout=GRACEFUL_STOP_SECONDS,
                )
            except asyncio.TimeoutError:
                print("ВНИМАНИЕ: часть воркеров не завершилась вовремя.", file=sys.stderr)

        rep.cancel()
        await asyncio.gather(rep, return_exceptions=True)

    total_time = time.perf_counter() - start_ts
    print("\n" + "=" * 80)
    print("  ИТОГ")
    print("=" * 80)
    print(f"  Время работы      : {total_time:.1f}s")
    print(f"  Всего запросов    : {stats.total}")
    print(f"  Успешных (2xx/3xx): {stats.success}")
    print(f"  Неуспешных (4/5xx): {stats.failed}")
    print(f"  Сетевых ошибок    : {stats.errors}")
    print(f"  Средний RPS       : {stats.total / total_time if total_time else 0:.1f}")
    print(f"  Средняя задержка  : {stats.avg_latency_ms:.1f} ms")
    if stats.status_codes:
        codes = ", ".join(f"{c}: {n}" for c, n in sorted(stats.status_codes.items()))
        print(f"  Коды ответов      : {codes}")
    success_pct = (stats.success / stats.total * 100) if stats.total else 0
    print(f"  Доля успешных     : {success_pct:.2f}%")

    # PDF-отчёт.
    if not args.no_report:
        report_path = args.report or os.path.join(
            REPORTS_DIR, f"loadtest_report_{started_at:%Y%m%d_%H%M%S}.pdf")
        try:
            generate_pdf_report(report_path, args, stats, total_time, started_at, fd_limit)
        except Exception as e:
            print(f"  Не удалось сформировать PDF-отчёт: {e}", file=sys.stderr)
    print("=" * 80)


def generate_pdf_report(path: str, args, stats: Stats, total_time: float,
                        started_at: datetime, fd_limit: int) -> None:
    """Сформировать многостраничный PDF-отчёт о прохождении теста.

    Требует matplotlib. Содержит: сводку параметров и результатов, графики во
    времени (соединения, RPS, успехи/ошибки, задержка) и распределение задержек
    с разбивкой по HTTP-кодам.
    """
    try:
        import matplotlib
        matplotlib.use("Agg")  # без GUI — работает на любом сервере
        import matplotlib.pyplot as plt
        from matplotlib.backends.backend_pdf import PdfPages
    except ImportError:
        print("\nPDF-отчёт пропущен: не установлен matplotlib (pip install matplotlib).",
              file=sys.stderr)
        return

    # Гарантируем существование папки отчёта (и для дефолта reports/, и для
    # явного --report путь/файл.pdf с вложенными каталогами).
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)

    hist = stats.history
    pct = stats.percentiles_ms()
    total = stats.total
    success_pct = (stats.success / total * 100) if total else 0
    avg_rps = total / total_time if total_time else 0
    min_ms = stats.min_latency * 1000 if stats.latency_count else 0

    from matplotlib.patches import FancyBboxPatch, Rectangle
    plt.rcParams.update({
        "font.family": "DejaVu Sans",
        "font.size": 10,
        "axes.titlecolor": "#1e293b",
        "axes.labelcolor": "#475569",
        "axes.edgecolor": "#cbd5e1",
        "xtick.color": "#64748b",
        "ytick.color": "#64748b",
        "grid.color": "#e2e8f0",
    })

    # Единая палитра отчёта.
    PRIMARY, ACCENT, GREEN = "#1e3a8a", "#2563eb", "#16a34a"
    AMBER, RED, PURPLE, MUTED = "#d97706", "#dc2626", "#7c3aed", "#94a3b8"

    def fmt_int(n):
        return f"{int(round(n)):,}".replace(",", " ")  # тонкий пробел-разделитель

    # Цвет доли успешных и итоговый вердикт.
    if total == 0:
        succ_color = vcolor = MUTED
        verdict = "НЕТ ДАННЫХ — ответов не получено"
    elif success_pct >= 99:
        succ_color = vcolor = GREEN
        verdict = "ОТЛИЧНО — бэкенд стабилен под нагрузкой"
    elif success_pct >= 95:
        succ_color = vcolor = GREEN
        verdict = "ХОРОШО — единичные ошибки"
    elif success_pct >= 80:
        succ_color = vcolor = AMBER
        verdict = "ЕСТЬ ПРОБЛЕМЫ — заметная доля ошибок"
    else:
        succ_color = vcolor = RED
        verdict = "КРИТИЧНО — высокая доля ошибок"

    total_pages = 4 if hist else 2

    def add_footer(fig, page):
        fig.text(0.04, 0.018, f"Сформировано loadtest.py · {started_at:%Y-%m-%d %H:%M}",
                 fontsize=7.5, color="#9ca3af", ha="left")
        fig.text(0.96, 0.018, f"Страница {page} из {total_pages}",
                 fontsize=7.5, color="#9ca3af", ha="right")

    def page_header(fig, title, subtitle=""):
        ax = fig.add_axes([0, 0, 1, 1]); ax.set_xlim(0, 1); ax.set_ylim(0, 1)
        ax.axis("off"); ax.patch.set_visible(False); ax.set_zorder(-1)
        ax.add_patch(Rectangle((0, 0.945), 1, 0.055, color=PRIMARY, lw=0))
        ax.add_patch(Rectangle((0, 0.940), 1, 0.005, color=ACCENT, lw=0))
        ax.text(0.04, 0.9725, title, ha="left", va="center",
                fontsize=15, fontweight="bold", color="white")
        if subtitle:
            ax.text(0.96, 0.9725, subtitle, ha="right", va="center",
                    fontsize=9, color="#cbd5e1")

    def style_ax(ax):
        ax.spines["top"].set_visible(False)
        ax.spines["right"].set_visible(False)
        ax.grid(True, alpha=0.6, linewidth=0.7)
        ax.tick_params(labelsize=8)

    def area(ax, x, y, color, lw=1.8):
        ax.plot(x, y, color=color, linewidth=lw)
        ax.fill_between(x, y, color=color, alpha=0.12)

    def styled_table(ax, rows, accent):
        ax.axis("off")
        t = ax.table(cellText=rows, colLabels=["Параметр", "Значение"],
                     colWidths=[0.62, 0.38], cellLoc="left", bbox=[0, 0, 1, 1])
        t.auto_set_font_size(False); t.set_fontsize(9.3)
        for (r, c), cell in t.get_celld().items():
            cell.set_edgecolor("#e5e7eb"); cell.set_linewidth(0.6)
            if r == 0:
                cell.set_facecolor(accent)
                cell.set_text_props(color="white", fontweight="bold")
            else:
                cell.set_facecolor("#ffffff" if r % 2 else "#f1f5f9")
                cell.set_text_props(color="#0f172a" if c == 1 else "#475569",
                                    fontweight="bold" if c == 1 else "normal")

    params = [
        ["Соединения (макс.)", fmt_int(args.connections)],
        ["Ramp-up", f"{args.ramp:g} с"],
        ["Заданная длительность", f"{args.duration:g} мин ({args.duration * 60:.0f} с)"],
        ["HTTP-метод", args.method],
        ["Таймаут запроса", f"{args.timeout:g} с"],
        ["Лимит дескрипторов", fmt_int(fd_limit) if fd_limit > 0 else "n/a"],
    ]
    results = [
        ["Фактическая длительность", f"{total_time:.1f} с"],
        ["Всего запросов", fmt_int(total)],
        ["Успешных (2xx/3xx)", fmt_int(stats.success)],
        ["Неуспешных (4xx/5xx)", fmt_int(stats.failed)],
        ["Сетевых ошибок", fmt_int(stats.errors)],
        ["Доля успешных", f"{success_pct:.2f} %"],
        ["Средний RPS", f"{avg_rps:.1f}"],
        ["Задержка средняя", f"{stats.avg_latency_ms:.1f} мс"],
        ["Задержка min / max", f"{min_ms:.1f} / {stats.max_latency * 1000:.1f} мс"],
    ]
    if pct:
        results.append(["Задержка p50 / p90", f"{pct['p50']:.1f} / {pct['p90']:.1f} мс"])
        results.append(["Задержка p95 / p99", f"{pct['p95']:.1f} / {pct['p99']:.1f} мс"])

    with PdfPages(path) as pdf:
        # ---------- Страница 1: титульная сводка ----------
        fig = plt.figure(figsize=(8.27, 11.69))  # A4 портрет
        axc = fig.add_axes([0, 0, 1, 1]); axc.set_xlim(0, 1); axc.set_ylim(0, 1)
        axc.axis("off"); axc.patch.set_visible(False)

        # Шапка
        axc.add_patch(Rectangle((0, 0.90), 1, 0.10, color=PRIMARY, lw=0))
        axc.add_patch(Rectangle((0, 0.892), 1, 0.008, color=ACCENT, lw=0))
        axc.text(0.06, 0.953, "Отчёт о нагрузочном тестировании",
                 fontsize=19, fontweight="bold", color="white", va="center")
        axc.text(0.06, 0.918, f"{args.method}  {args.url}",
                 fontsize=10.5, color="#bfdbfe", va="center")
        axc.text(0.94, 0.953, f"{started_at:%d.%m.%Y}", fontsize=10, color="#bfdbfe",
                 ha="right", va="center")
        axc.text(0.94, 0.920, f"{started_at:%H:%M:%S}", fontsize=9, color="#93c5fd",
                 ha="right", va="center")

        # KPI-карточки
        cards = [
            ("Всего запросов", fmt_int(total), ACCENT),
            ("Доля успешных", f"{success_pct:.1f}%", succ_color),
            ("Средний RPS", fmt_int(avg_rps), PURPLE),
            ("p95 задержка" if pct else "Средняя задержка",
             f"{(pct['p95'] if pct else stats.avg_latency_ms):.0f} мс", AMBER),
        ]
        cw, gap, cy, ch = 0.205, 0.0267, 0.775, 0.085
        for i, (label, value, color) in enumerate(cards):
            x = 0.06 + i * (cw + gap)
            axc.add_patch(FancyBboxPatch((x, cy), cw, ch,
                          boxstyle="round,pad=0.004,rounding_size=0.010",
                          linewidth=1.0, edgecolor="#e5e7eb", facecolor="white"))
            axc.add_patch(Rectangle((x + 0.012, cy + ch - 0.014), cw - 0.024, 0.006,
                          color=color, lw=0))
            axc.text(x + cw / 2, cy + ch * 0.50, value, ha="center", va="center",
                     fontsize=16, fontweight="bold", color=color)
            axc.text(x + cw / 2, cy + ch * 0.18, label, ha="center", va="center",
                     fontsize=8.3, color="#6b7280")

        # Вердикт
        axc.add_patch(FancyBboxPatch((0.06, 0.694), 0.88, 0.05,
                      boxstyle="round,pad=0.004,rounding_size=0.010",
                      linewidth=1.2, edgecolor=vcolor, facecolor=vcolor, alpha=0.12))
        axc.text(0.085, 0.719, "ВЕРДИКТ", fontsize=9, fontweight="bold",
                 color=vcolor, va="center")
        axc.text(0.205, 0.719, verdict, fontsize=11.5, color="#0f172a", va="center")

        # Таблицы
        axc.text(0.06, 0.665, "Параметры теста", fontsize=12, fontweight="bold", color=PRIMARY)
        axc.text(0.06, 0.435, "Итоговые результаты", fontsize=12, fontweight="bold", color=PRIMARY)
        styled_table(fig.add_axes([0.06, 0.475, 0.88, 0.175]), params, PRIMARY)
        styled_table(fig.add_axes([0.06, 0.075, 0.88, 0.345]), results, PRIMARY)

        add_footer(fig, 1)
        pdf.savefig(fig)
        plt.close(fig)

        # ---------- Страница 2: динамика во времени ----------
        if hist:
            ts = [h["t"] for h in hist]
            fig, axes = plt.subplots(2, 2, figsize=(11.69, 8.27))  # A4 альбом
            fig.subplots_adjust(top=0.88, bottom=0.10, left=0.07, right=0.96,
                                hspace=0.33, wspace=0.18)
            page_header(fig, "Динамика во времени", f"{args.method} {args.url}")

            area(axes[0, 0], ts, [h["active"] for h in hist], ACCENT)
            axes[0, 0].set_title("Активные соединения", loc="left", fontweight="bold", color=ACCENT)
            axes[0, 0].set_ylabel("conn")

            area(axes[0, 1], ts, [h["rps"] for h in hist], GREEN)
            axes[0, 1].set_title("RPS (интервальный)", loc="left", fontweight="bold", color=GREEN)
            axes[0, 1].set_ylabel("запросов/с")

            axes[1, 0].plot(ts, [h["success"] for h in hist], label="OK", color=GREEN, lw=1.8)
            axes[1, 0].plot(ts, [h["failed"] for h in hist], label="FAIL", color=AMBER, lw=1.8)
            axes[1, 0].plot(ts, [h["errors"] for h in hist], label="ERR", color=RED, lw=1.8)
            axes[1, 0].set_title("Ответы накопительно", loc="left", fontweight="bold", color="#1e293b")
            axes[1, 0].set_ylabel("шт.")
            axes[1, 0].legend(fontsize=8, frameon=False)

            area(axes[1, 1], ts, [h["avg_ms"] for h in hist], PURPLE)
            axes[1, 1].set_title("Средняя задержка", loc="left", fontweight="bold", color=PURPLE)
            axes[1, 1].set_ylabel("мс")

            for ax in axes.flat:
                ax.set_xlabel("время, с")
                style_ax(ax)
            add_footer(fig, 2)
            pdf.savefig(fig)
            plt.close(fig)

        # ---------- Страница 3: ошибки во времени ----------
        if hist:
            # Интервальные значения считаем как приращения накопительных метрик
            # между соседними точками временного ряда.
            e_ts, fail_ps, err_ps, fail_cum, err_cum = [], [], [], [], []
            err_pct_int, err_pct_cum = [], []
            prev = None
            for h in hist:
                if prev is not None:
                    dt = h["t"] - prev["t"]
                    if dt > 0:
                        d_fail = h["failed"] - prev["failed"]
                        d_err = h["errors"] - prev["errors"]
                        d_tot = ((h["success"] + h["failed"] + h["errors"])
                                 - (prev["success"] + prev["failed"] + prev["errors"]))
                        e_ts.append(h["t"])
                        fail_ps.append(d_fail / dt)
                        err_ps.append(d_err / dt)
                        err_pct_int.append((d_fail + d_err) / d_tot * 100 if d_tot > 0 else 0)
                cum_tot = h["success"] + h["failed"] + h["errors"]
                fail_cum.append(h["failed"])
                err_cum.append(h["errors"])
                err_pct_cum.append((h["failed"] + h["errors"]) / cum_tot * 100 if cum_tot else 0)
                prev = h

            fig, axes = plt.subplots(2, 2, figsize=(11.69, 8.27))  # A4 альбом
            fig.subplots_adjust(top=0.88, bottom=0.10, left=0.07, right=0.96,
                                hspace=0.33, wspace=0.18)
            page_header(fig, "Ошибки во времени", f"{args.method} {args.url}")

            axes[0, 0].plot(e_ts, fail_ps, label="FAIL/с (4xx/5xx)", color=AMBER, lw=1.8)
            axes[0, 0].plot(e_ts, err_ps, label="ERR/с (сетевые)", color=RED, lw=1.8)
            axes[0, 0].set_title("Интенсивность ошибок (в секунду)", loc="left",
                                 fontweight="bold", color="#1e293b")
            axes[0, 0].set_ylabel("ошибок/с")
            axes[0, 0].legend(fontsize=8, frameon=False)

            area(axes[0, 1], e_ts, err_pct_int, RED)
            axes[0, 1].set_title("Доля ошибок за интервал", loc="left", fontweight="bold", color=RED)
            axes[0, 1].set_ylabel("%")
            axes[0, 1].set_ylim(bottom=0)

            ts_all = [h["t"] for h in hist]
            axes[1, 0].plot(ts_all, fail_cum, label="FAIL", color=AMBER, lw=1.8)
            axes[1, 0].plot(ts_all, err_cum, label="ERR", color=RED, lw=1.8)
            axes[1, 0].set_title("Ошибки накопительно", loc="left", fontweight="bold", color="#1e293b")
            axes[1, 0].set_ylabel("шт.")
            axes[1, 0].legend(fontsize=8, frameon=False)

            area(axes[1, 1], ts_all, err_pct_cum, PURPLE)
            axes[1, 1].set_title("Доля ошибок накопительно", loc="left", fontweight="bold", color=PURPLE)
            axes[1, 1].set_ylabel("%")
            axes[1, 1].set_ylim(bottom=0)

            for ax in axes.flat:
                ax.set_xlabel("время, с")
                style_ax(ax)
            add_footer(fig, 3)
            pdf.savefig(fig)
            plt.close(fig)

        # ---------- Страница распределения ----------
        fig, (axL, axR) = plt.subplots(1, 2, figsize=(11.69, 8.27))
        fig.subplots_adjust(top=0.86, bottom=0.12, left=0.07, right=0.96, wspace=0.18)
        page_header(fig, "Распределение ответов и задержек", f"{args.method} {args.url}")

        if stats.status_codes:
            codes = sorted(stats.status_codes.items())
            labels = [str(c) for c, _ in codes]
            counts = [n for _, n in codes]
            colors = [GREEN if 200 <= c < 400 else (AMBER if c < 500 else RED) for c, _ in codes]
            bars = axL.bar(labels, counts, color=colors, width=0.6, zorder=3)
            axL.set_title("HTTP-коды ответов", loc="left", fontweight="bold", color="#1e293b")
            axL.set_xlabel("код"); axL.set_ylabel("шт.")
            axL.bar_label(bars, labels=[fmt_int(n) for n in counts], padding=3, fontsize=8.5)
            axL.margins(y=0.12)
        else:
            axL.axis("off"); axL.text(0.5, 0.5, "Нет ответов", ha="center")

        if stats.latencies:
            axR.hist([x * 1000 for x in stats.latencies], bins=40, color=ACCENT,
                     edgecolor="white", alpha=0.85, zorder=3)
            axR.set_title(f"Гистограмма задержек (выборка {fmt_int(len(stats.latencies))})",
                          loc="left", fontweight="bold", color=ACCENT)
            axR.set_xlabel("задержка, мс"); axR.set_ylabel("частота")
            if pct:
                axR.axvline(pct["p95"], color=RED, linestyle="--", linewidth=1.4,
                            label=f"p95 = {pct['p95']:.0f} мс", zorder=4)
                axR.legend(fontsize=8.5, frameon=False)
        else:
            axR.axis("off"); axR.text(0.5, 0.5, "Нет данных о задержках", ha="center")

        for ax in (axL, axR):
            style_ax(ax)
        add_footer(fig, total_pages)
        pdf.savefig(fig)
        plt.close(fig)

        d = pdf.infodict()
        d["Title"] = "Отчёт о нагрузочном тестировании"
        d["Subject"] = f"{args.method} {args.url}"
        d["CreationDate"] = started_at

    print(f"  PDF-отчёт         : {path}")


def main():
    p = argparse.ArgumentParser(
        description="Нагрузочное тестирование бэкенда (async).",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    p.add_argument("--url", required=True, help="URL бэкенда")
    p.add_argument("--connections", "-c", type=int, default=50,
                   help="Максимальное число параллельных соединений")
    p.add_argument("--duration", "-d", type=float, default=1.0,
                   help="Время работы в минутах")
    p.add_argument("--ramp", type=float, default=10.0,
                   help="За сколько секунд нарастить соединения от 0 до connections")
    p.add_argument("--method", "-m", default="GET", help="HTTP-метод")
    p.add_argument("--header", "-H", action="append", default=[],
                   help="Заголовок 'Key: Value' (можно несколько раз)")
    p.add_argument("--body", "-b", default=None, help="Тело запроса (для POST/PUT)")
    p.add_argument("--timeout", "-t", type=float, default=30.0,
                   help="Таймаут одного запроса, секунд")
    p.add_argument("--report", default=None,
                   help="Путь к PDF-отчёту (по умолчанию reports/loadtest_report_<дата>.pdf)")
    p.add_argument("--no-report", action="store_true",
                   help="Не формировать PDF-отчёт")
    args = p.parse_args()

    if args.connections < 1:
        sys.exit("--connections должно быть >= 1")

    try:
        asyncio.run(run(args))
    except KeyboardInterrupt:
        print("\nПрервано пользователем.")


if __name__ == "__main__":
    main()
