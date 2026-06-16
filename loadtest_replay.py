#!/usr/bin/env python3
"""
Нагрузочное тестирование бэкенда по реальным данным из JSONL-файла.

Считывает записи из файла (tests/curse_breaker_usn.filtered или другого),
маппит каждую запись на endpoint по полю Tag, строит пул запросов и в
--connections параллельных соединений непрерывно шлёт случайные запросы
из пула, измеряя RPS и задержку как по всему трафику, так и по каждому
endpoint отдельно.

Зависимости:
    pip install aiohttp matplotlib

Примеры запуска:

    # Полный replay — все endpoint-ы вместе:
    python3 loadtest_replay.py \\
        --base-url http://10.0.8.49:7777 \\
        --api-key bee03ffd-3c6f-4e88-8b7b-b4ac673c9cec \\
        --connections 100 --duration 5

    # Изоляция одного endpoint:
    python3 loadtest_replay.py \\
        --base-url http://10.0.8.49:7777 \\
        --api-key bee03ffd-3c6f-4e88-8b7b-b4ac673c9cec \\
        --connections 50 --tag GetFnsFlowFullInfo

    # Только чтение (без POST /operations):
    python3 loadtest_replay.py \\
        --base-url http://10.0.8.49:7777 \\
        --api-key bee03ffd-3c6f-4e88-8b7b-b4ac673c9cec \\
        --connections 100 --exclude-tag GetOperations
"""

from __future__ import annotations

import argparse
import asyncio
import json
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

LATENCY_RESERVOIR_CAP = 50000
TAG_LATENCY_CAP = 10000
GRACEFUL_STOP_SECONDS = 5.0
REPORTS_DIR = "reports"

TAG_MAP: dict[str, tuple[str, str]] = {
    "GetUser":            ("GET",  "/users"),
    "GetSourcesInfo":     ("GET",  "/sources"),
    "GetOperations":      ("POST", "/operations"),
    "GetTasks":           ("GET",  "/tasks"),
    "ListCompletedTasks": ("GET",  "/tasks/completed"),
    "GetTaxLimits":       ("GET",  "/references/tax_limits"),
    "GetOperationById":   ("GET",  "/operations/{OperationID}"),
    "GetSourceState":     ("GET",  "/sources/{RequestID}/state"),
    "GetFnsFlowFullInfo": ("GET",  "/fns_reports/flows/{FlowID}/detailed"),
}


# ---------------------------------------------------------------------------
# Утилиты (из loadtest.py)
# ---------------------------------------------------------------------------

def raise_fd_limit(target: int = 200000) -> int:
    try:
        import resource
    except ImportError:
        return -1
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
    try:
        with open("/proc/sys/net/ipv4/ip_local_port_range") as f:
            lo, hi = (int(x) for x in f.read().split())
            return hi - lo + 1
    except OSError:
        pass
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
    ports = ephemeral_port_count()
    if 0 < ports <= connections:
        print(
            f"  ВНИМАНИЕ: запрошено {connections} соединений, но к одному адресу:порту\n"
            f"           доступно лишь ~{ports} портов-источников.",
            file=sys.stderr,
        )
    if connections > 10000:
        print(
            f"  ВНИМАНИЕ: {connections} соединений — очень много для одного процесса asyncio.",
            file=sys.stderr,
        )


# ---------------------------------------------------------------------------
# Статистика
# ---------------------------------------------------------------------------

@dataclass
class Stats:
    """Метрики теста: суммарные и разбивка по Tag."""
    active: int = 0
    success: int = 0
    failed: int = 0
    errors: int = 0
    total_latency: float = 0.0
    latency_count: int = 0
    min_latency: float = float("inf")
    max_latency: float = 0.0
    status_codes: dict = field(default_factory=dict)
    latencies: list = field(default_factory=list)
    history: list = field(default_factory=list)
    # Per-tag counters
    tag_success: dict = field(default_factory=dict)
    tag_failed: dict = field(default_factory=dict)
    tag_errors: dict = field(default_factory=dict)
    tag_total_latency: dict = field(default_factory=dict)
    tag_latency_count: dict = field(default_factory=dict)
    tag_latencies: dict = field(default_factory=dict)

    def record_latency(self, sec: float) -> None:
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

    def record_tag(self, tag: str, sec: float, status: int) -> None:
        if 200 <= status < 400:
            self.tag_success[tag] = self.tag_success.get(tag, 0) + 1
        else:
            self.tag_failed[tag] = self.tag_failed.get(tag, 0) + 1
        self.tag_total_latency[tag] = self.tag_total_latency.get(tag, 0.0) + sec
        count = self.tag_latency_count.get(tag, 0) + 1
        self.tag_latency_count[tag] = count
        lats = self.tag_latencies.setdefault(tag, [])
        if len(lats) < TAG_LATENCY_CAP:
            lats.append(sec)
        else:
            j = random.randint(0, count - 1)
            if j < TAG_LATENCY_CAP:
                lats[j] = sec

    def record_tag_error(self, tag: str) -> None:
        self.tag_errors[tag] = self.tag_errors.get(tag, 0) + 1

    def tag_total_requests(self, tag: str) -> int:
        return (self.tag_success.get(tag, 0)
                + self.tag_failed.get(tag, 0)
                + self.tag_errors.get(tag, 0))

    def tag_avg_latency_ms(self, tag: str) -> float:
        c = self.tag_latency_count.get(tag, 0)
        if not c:
            return 0.0
        return self.tag_total_latency.get(tag, 0.0) / c * 1000

    def tag_percentiles_ms(self, tag: str) -> dict:
        lats = self.tag_latencies.get(tag, [])
        if not lats:
            return {}
        s = sorted(lats)
        n = len(s)

        def pct(p):
            return s[min(n - 1, max(0, int(round(p / 100 * (n - 1)))))] * 1000

        return {"p50": pct(50), "p90": pct(90), "p95": pct(95), "p99": pct(99)}

    @property
    def total(self) -> int:
        return self.success + self.failed + self.errors

    @property
    def avg_latency_ms(self) -> float:
        return (self.total_latency / self.latency_count * 1000) if self.latency_count else 0.0

    def percentiles_ms(self) -> dict:
        if not self.latencies:
            return {}
        s = sorted(self.latencies)
        n = len(s)

        def pct(p):
            return s[min(n - 1, max(0, int(round(p / 100 * (n - 1)))))] * 1000

        return {"p50": pct(50), "p90": pct(90), "p95": pct(95), "p99": pct(99)}


# ---------------------------------------------------------------------------
# Пул запросов
# ---------------------------------------------------------------------------

def build_request_pool(
    file_path: str,
    base_url: str,
    api_key: str,
    tag_filter: list[str],
    exclude_tags: list[str],
) -> list[dict]:
    """Читает JSONL-файл и строит список запросов (method, url, headers, body, tag)."""
    base_url = base_url.rstrip("/")
    pool: list[dict] = []
    skipped = 0

    with open(file_path, encoding="utf-8") as f:
        for line_num, raw in enumerate(f, 1):
            raw = raw.strip()
            if not raw:
                continue
            try:
                rec = json.loads(raw)
            except json.JSONDecodeError as e:
                print(f"  ПРОПУЩЕНА строка {line_num}: {e}", file=sys.stderr)
                skipped += 1
                continue

            tag = rec.get("Tag", "")
            if tag not in TAG_MAP:
                skipped += 1
                continue
            if tag_filter and tag not in tag_filter:
                continue
            if exclude_tags and tag in exclude_tags:
                continue

            method, path_tpl = TAG_MAP[tag]
            path = path_tpl.format(
                OperationID=rec.get("OperationID", ""),
                RequestID=rec.get("RequestID", ""),
                FlowID=rec.get("FlowID", ""),
                SourceID=rec.get("SourceID", ""),
            )
            headers = {
                "accept": "application/json",
                "x-client-id": rec.get("ClientID", ""),
                "x-api-key": api_key,
            }
            body: str | None = None
            if method == "POST":
                headers["content-type"] = "application/json"
                body = json.dumps({
                    "inn": rec.get("Inn", ""),
                    "tax_rate": rec.get("TaxRate", 0),
                    "tax_system": rec.get("TaxSystem", ""),
                    "start_year": rec.get("StartYear", 0),
                })

            pool.append({"tag": tag, "method": method, "url": base_url + path,
                         "headers": headers, "body": body})

    if skipped:
        print(f"  Пропущено строк (неизвестный/отфильтрованный Tag): {skipped}", file=sys.stderr)
    return pool


# ---------------------------------------------------------------------------
# Последовательный пул (режим --sequential)
# ---------------------------------------------------------------------------

class SequentialPool:
    """Выдаёт запросы из пула по порядку, зацикливаясь после последнего.

    Потокобезопасен в asyncio: между await-точками воркера работает ровно
    одна корутина, поэтому инкремент индекса атомарен без блокировок.
    """

    def __init__(self, pool: list[dict]) -> None:
        self._pool = pool
        self._index = 0
        self.passes = 0  # сколько полных проходов по файлу завершено

    def next(self) -> dict:
        req = self._pool[self._index]
        self._index += 1
        if self._index >= len(self._pool):
            self._index = 0
            self.passes += 1
        return req


# ---------------------------------------------------------------------------
# Воркер / Репортер / Ramp-up
# ---------------------------------------------------------------------------

async def worker(
    session: aiohttp.ClientSession,
    get_req,  # callable: () -> dict
    timeout: float,
    stop_event: asyncio.Event,
    stats: Stats,
) -> None:
    stats.active += 1
    try:
        while not stop_event.is_set():
            req = get_req()
            start = time.perf_counter()
            try:
                async with session.request(
                    req["method"],
                    req["url"],
                    headers=req["headers"],
                    data=req["body"],
                    timeout=aiohttp.ClientTimeout(total=timeout),
                ) as resp:
                    await resp.read()
                    elapsed = time.perf_counter() - start
                    stats.record_latency(elapsed)
                    stats.record_tag(req["tag"], elapsed, resp.status)
                    stats.status_codes[resp.status] = stats.status_codes.get(resp.status, 0) + 1
                    if 200 <= resp.status < 400:
                        stats.success += 1
                    else:
                        stats.failed += 1
            except asyncio.CancelledError:
                raise
            except Exception:
                stats.errors += 1
                stats.record_tag_error(req["tag"])
    finally:
        stats.active -= 1


async def reporter(
    stop_event: asyncio.Event,
    stats: Stats,
    start_ts: float,
    duration: float,
    tags: list[str],
) -> None:
    last_total = 0
    last_t = 0.0
    while not stop_event.is_set():
        elapsed = time.perf_counter() - start_ts
        dt = elapsed - last_t
        rps_interval = (stats.total - last_total) / dt if dt > 0 else 0
        last_total, last_t = stats.total, elapsed
        remaining = max(0.0, duration - elapsed)

        stats.history.append({
            "t": elapsed,
            "active": stats.active,
            "success": stats.success,
            "failed": stats.failed,
            "errors": stats.errors,
            "rps": rps_interval,
            "avg_ms": stats.avg_latency_ms,
            "tag_totals": {tag: stats.tag_total_requests(tag) for tag in tags},
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
    session: aiohttp.ClientSession,
    get_req,  # callable: () -> dict
    timeout: float,
    connections: int,
    ramp_seconds: float,
    stop_event: asyncio.Event,
    stats: Stats,
    worker_tasks: list,
) -> None:
    start = time.perf_counter()
    for i in range(connections):
        if stop_event.is_set():
            break
        worker_tasks.append(asyncio.create_task(
            worker(session, get_req, timeout, stop_event, stats)
        ))
        if ramp_seconds > 0 and i + 1 < connections:
            target = start + ramp_seconds * (i + 1) / connections
            sleep_for = target - time.perf_counter()
            if sleep_for > 0:
                try:
                    await asyncio.wait_for(stop_event.wait(), timeout=sleep_for)
                    break
                except asyncio.TimeoutError:
                    pass


# ---------------------------------------------------------------------------
# Основная функция запуска
# ---------------------------------------------------------------------------

async def run(args) -> None:
    pool = build_request_pool(
        args.file, args.base_url, args.api_key,
        args.tag or [], args.exclude_tag or [],
    )
    if not pool:
        sys.exit("Пул запросов пуст — проверьте --file, --tag, --exclude-tag.")

    tags = sorted({r["tag"] for r in pool})
    duration = args.duration * 60.0
    started_at = datetime.now()
    stop_event = asyncio.Event()

    if args.sequential:
        seq_pool = SequentialPool(pool)
        get_req = seq_pool.next
    else:
        seq_pool = None
        get_req = lambda: random.choice(pool)

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, stop_event.set)
        except NotImplementedError:
            pass

    fd_limit = raise_fd_limit()
    if 0 < fd_limit <= args.connections + 50:
        print(f"  ВНИМАНИЕ: лимит дескрипторов ({fd_limit}) близок к числу соединений "
              f"({args.connections}). Поднимите: ulimit -n 200000", file=sys.stderr)
    warn_about_scale(args.connections)

    stats = Stats()
    connector = aiohttp.TCPConnector(limit=args.connections, ssl=False)

    mode_label = "последовательный (файл → рестарт)" if args.sequential else "случайный"
    print("=" * 80)
    print(f"  Base URL    : {args.base_url}")
    print(f"  Режим       : {mode_label}")
    print(f"  Tags        : {', '.join(tags)}")
    print(f"  Pool size   : {len(pool)} запросов")
    print(f"  Connections : {args.connections} (ramp-up {args.ramp}s)")
    print(f"  Duration    : {args.duration} мин ({duration:.0f}s)")
    print(f"  Timeout     : {args.timeout}s/запрос")
    print(f"  FD limit    : {fd_limit if fd_limit > 0 else 'n/a'}")
    print("=" * 80)

    async with aiohttp.ClientSession(connector=connector) as session:
        start_ts = time.perf_counter()
        rep = asyncio.create_task(reporter(stop_event, stats, start_ts, duration, tags))

        worker_tasks: list = []
        ramp_task = asyncio.create_task(ramp_up(
            session, get_req, args.timeout,
            args.connections, args.ramp, stop_event, stats, worker_tasks,
        ))

        try:
            await asyncio.wait_for(stop_event.wait(), timeout=duration)
        except asyncio.TimeoutError:
            pass

        stop_event.set()
        ramp_task.cancel()
        await asyncio.gather(ramp_task, return_exceptions=True)

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
    _print_summary(stats, total_time, tags, seq_pool)

    if not args.no_report:
        report_path = args.report or os.path.join(
            REPORTS_DIR, f"loadtest_report_{started_at:%Y%m%d_%H%M%S}.pdf")
        try:
            generate_pdf_report(report_path, args, stats, total_time, started_at, fd_limit, tags)
        except Exception as e:
            print(f"  Не удалось сформировать PDF-отчёт: {e}", file=sys.stderr)
    print("=" * 80)


def _print_summary(
    stats: Stats, total_time: float, tags: list[str],
    seq_pool: SequentialPool | None = None,
) -> None:
    print("\n" + "=" * 80)
    print("  ИТОГ — СУММАРНО")
    print("=" * 80)
    total = stats.total
    print(f"  Время работы      : {total_time:.1f}s")
    print(f"  Всего запросов    : {total}")
    print(f"  Успешных (2xx/3xx): {stats.success}")
    print(f"  Неуспешных (4/5xx): {stats.failed}")
    print(f"  Сетевых ошибок    : {stats.errors}")
    print(f"  Средний RPS       : {total / total_time if total_time else 0:.1f}")
    print(f"  Средняя задержка  : {stats.avg_latency_ms:.1f} ms")
    pct = stats.percentiles_ms()
    if pct:
        print(f"  Задержка p50/p95/p99: {pct['p50']:.0f} / {pct['p95']:.0f} / {pct['p99']:.0f} ms")
    if stats.status_codes:
        codes = ", ".join(f"{c}: {n}" for c, n in sorted(stats.status_codes.items()))
        print(f"  Коды ответов      : {codes}")
    success_pct = (stats.success / total * 100) if total else 0
    print(f"  Доля успешных     : {success_pct:.2f}%")
    if seq_pool is not None:
        print(f"  Проходов по файлу : {seq_pool.passes} полных + {seq_pool._index} запросов в текущем")

    print("\n  РАЗБИВКА ПО ENDPOINT")
    print(f"  {'Tag':<26} {'Запросов':>9} {'OK':>7} {'FAIL':>7} {'ERR':>7} {'RPS':>7} {'p95мс':>8}")
    print("  " + "-" * 75)
    for tag in tags:
        n = stats.tag_total_requests(tag)
        ok = stats.tag_success.get(tag, 0)
        fail = stats.tag_failed.get(tag, 0)
        err = stats.tag_errors.get(tag, 0)
        rps = n / total_time if total_time else 0
        tp = stats.tag_percentiles_ms(tag)
        p95 = f"{tp['p95']:.0f}" if tp else "—"
        print(f"  {tag:<26} {n:>9} {ok:>7} {fail:>7} {err:>7} {rps:>7.1f} {p95:>8}")


# ---------------------------------------------------------------------------
# PDF-отчёт
# ---------------------------------------------------------------------------

def generate_pdf_report(
    path: str, args, stats: Stats, total_time: float,
    started_at: datetime, fd_limit: int, tags: list[str],
) -> None:
    try:
        import matplotlib
        matplotlib.use("Agg")
        import matplotlib.pyplot as plt
        from matplotlib.backends.backend_pdf import PdfPages
        from matplotlib.patches import FancyBboxPatch, Rectangle
    except ImportError:
        print("\nPDF-отчёт пропущен: не установлен matplotlib (pip install matplotlib).",
              file=sys.stderr)
        return

    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)

    hist = stats.history
    pct = stats.percentiles_ms()
    total = stats.total
    success_pct = (stats.success / total * 100) if total else 0
    avg_rps = total / total_time if total_time else 0
    min_ms = stats.min_latency * 1000 if stats.latency_count else 0
    has_hist = bool(hist)
    has_tags = bool(tags)

    # Считаем итоговое количество страниц
    total_pages = 1  # титульная
    if has_hist:
        total_pages += 2  # динамика + ошибки
    total_pages += 1  # распределение
    if has_tags:
        total_pages += 2  # per-tag таблица + latency comparison
        if has_hist:
            total_pages += 1  # tag time-series

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

    PRIMARY, ACCENT, GREEN = "#1e3a8a", "#2563eb", "#16a34a"
    AMBER, RED, PURPLE, MUTED = "#d97706", "#dc2626", "#7c3aed", "#94a3b8"

    TAG_COLORS = [ACCENT, GREEN, AMBER, RED, PURPLE, "#0891b2", "#b45309", "#be185d", "#065f46"]

    def fmt_int(n):
        return f"{int(round(n)):,}".replace(",", " ")

    def fmt_endpoints(t):
        if not t or len(t) == 9:
            return "все (9 endpoint-ов)"
        if len(t) <= 3:
            return ", ".join(t)
        return f"{len(t)} endpoint-ов: " + ", ".join(t[:3]) + f" +{len(t) - 3}"

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

    page_num = [0]

    def add_footer(fig):
        page_num[0] += 1
        fig.text(0.04, 0.018,
                 f"Сформировано loadtest_replay.py · {started_at:%Y-%m-%d %H:%M}",
                 fontsize=7.5, color="#9ca3af", ha="left")
        fig.text(0.96, 0.018, f"Страница {page_num[0]} из {total_pages}",
                 fontsize=7.5, color="#9ca3af", ha="right")

    def page_header(fig, title, subtitle=""):
        ax = fig.add_axes([0, 0, 1, 1])
        ax.set_xlim(0, 1); ax.set_ylim(0, 1)
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

    def styled_table(ax, rows, col_labels, col_widths, accent=PRIMARY):
        ax.axis("off")
        t = ax.table(cellText=rows, colLabels=col_labels,
                     colWidths=col_widths, cellLoc="left", bbox=[0, 0, 1, 1])
        t.auto_set_font_size(False)
        t.set_fontsize(8.5)
        for (r, c), cell in t.get_celld().items():
            cell.set_edgecolor("#e5e7eb"); cell.set_linewidth(0.6)
            if r == 0:
                cell.set_facecolor(accent)
                cell.set_text_props(color="white", fontweight="bold")
            else:
                cell.set_facecolor("#ffffff" if r % 2 else "#f1f5f9")
                cell.set_text_props(
                    color="#0f172a" if c > 0 else "#475569",
                    fontweight="bold" if c > 0 else "normal",
                )

    with PdfPages(path) as pdf:

        # ------------------------------------------------------------------ #
        # Страница 1: Титульная сводка                                        #
        # ------------------------------------------------------------------ #
        fig = plt.figure(figsize=(8.27, 11.69))
        axc = fig.add_axes([0, 0, 1, 1])
        axc.set_xlim(0, 1); axc.set_ylim(0, 1)
        axc.axis("off"); axc.patch.set_visible(False)

        axc.add_patch(Rectangle((0, 0.90), 1, 0.10, color=PRIMARY, lw=0))
        axc.add_patch(Rectangle((0, 0.892), 1, 0.008, color=ACCENT, lw=0))
        axc.text(0.06, 0.953, "Отчёт о нагрузочном тестировании (replay)",
                 fontsize=17, fontweight="bold", color="white", va="center")
        axc.text(0.06, 0.918, args.base_url,
                 fontsize=10.5, color="#bfdbfe", va="center")
        axc.text(0.94, 0.953, f"{started_at:%d.%m.%Y}", fontsize=10, color="#bfdbfe",
                 ha="right", va="center")
        axc.text(0.94, 0.920, f"{started_at:%H:%M:%S}", fontsize=9, color="#93c5fd",
                 ha="right", va="center")

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

        axc.add_patch(FancyBboxPatch((0.06, 0.694), 0.88, 0.050,
                      boxstyle="round,pad=0.004,rounding_size=0.010",
                      linewidth=1.2, edgecolor=vcolor, facecolor=vcolor, alpha=0.12))
        axc.text(0.085, 0.719, "ВЕРДИКТ", fontsize=9, fontweight="bold",
                 color=vcolor, va="center")
        axc.text(0.205, 0.719, verdict, fontsize=11.5, color="#0f172a", va="center")

        params = [
            ["Base URL", args.base_url],
            ["Endpoint-ы", fmt_endpoints(tags)],
            ["Файл данных", args.file],
            ["Соединений (макс.)", fmt_int(args.connections)],
            ["Ramp-up", f"{args.ramp:g} с"],
            ["Длительность", f"{args.duration:g} мин ({args.duration * 60:.0f} с)"],
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

        axc.text(0.06, 0.665, "Параметры теста", fontsize=12, fontweight="bold", color=PRIMARY)
        axc.text(0.06, 0.400, "Итоговые результаты", fontsize=12, fontweight="bold", color=PRIMARY)
        styled_table(fig.add_axes([0.06, 0.440, 0.88, 0.210]),
                     params, ["Параметр", "Значение"], [0.55, 0.45])
        styled_table(fig.add_axes([0.06, 0.055, 0.88, 0.330]),
                     results, ["Показатель", "Значение"], [0.55, 0.45])

        add_footer(fig)
        pdf.savefig(fig)
        plt.close(fig)

        # ------------------------------------------------------------------ #
        # Страница 2: Динамика во времени                                     #
        # ------------------------------------------------------------------ #
        if has_hist:
            ts = [h["t"] for h in hist]
            fig, axes = plt.subplots(2, 2, figsize=(11.69, 8.27))
            fig.subplots_adjust(top=0.88, bottom=0.10, left=0.07, right=0.96,
                                hspace=0.33, wspace=0.18)
            page_header(fig, "Динамика во времени", args.base_url)

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
            add_footer(fig)
            pdf.savefig(fig)
            plt.close(fig)

        # ------------------------------------------------------------------ #
        # Страница 3: Ошибки во времени                                       #
        # ------------------------------------------------------------------ #
        if has_hist:
            e_ts, fail_ps, err_ps = [], [], []
            err_pct_int, fail_cum, err_cum, err_pct_cum = [], [], [], []
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

            fig, axes = plt.subplots(2, 2, figsize=(11.69, 8.27))
            fig.subplots_adjust(top=0.88, bottom=0.10, left=0.07, right=0.96,
                                hspace=0.33, wspace=0.18)
            page_header(fig, "Ошибки во времени", args.base_url)

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
            add_footer(fig)
            pdf.savefig(fig)
            plt.close(fig)

        # ------------------------------------------------------------------ #
        # Страница 4: Распределение HTTP-кодов и задержек                    #
        # ------------------------------------------------------------------ #
        fig, (axL, axR) = plt.subplots(1, 2, figsize=(11.69, 8.27))
        fig.subplots_adjust(top=0.86, bottom=0.12, left=0.07, right=0.96, wspace=0.18)
        page_header(fig, "Распределение ответов и задержек", args.base_url)

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
        add_footer(fig)
        pdf.savefig(fig)
        plt.close(fig)

        # ------------------------------------------------------------------ #
        # Страница 5: Per-endpoint сводная таблица                           #
        # ------------------------------------------------------------------ #
        if has_tags:
            fig = plt.figure(figsize=(11.69, 8.27))
            page_header(fig, "Разбивка по endpoint", args.base_url)

            col_labels = ["Tag (endpoint)", "Запросов", "OK", "FAIL", "ERR",
                          "RPS", "p50 мс", "p95 мс", "p99 мс"]
            col_widths = [0.22, 0.09, 0.08, 0.08, 0.08, 0.08, 0.09, 0.09, 0.09]
            rows = []
            for tag in tags:
                n = stats.tag_total_requests(tag)
                ok = stats.tag_success.get(tag, 0)
                fail = stats.tag_failed.get(tag, 0)
                err = stats.tag_errors.get(tag, 0)
                rps = n / total_time if total_time else 0
                tp = stats.tag_percentiles_ms(tag)
                p50 = f"{tp['p50']:.0f}" if tp else "—"
                p95 = f"{tp['p95']:.0f}" if tp else "—"
                p99 = f"{tp['p99']:.0f}" if tp else "—"
                rows.append([tag, fmt_int(n), fmt_int(ok), fmt_int(fail), fmt_int(err),
                             f"{rps:.1f}", p50, p95, p99])

            ax = fig.add_axes([0.04, 0.10, 0.92, 0.76])
            styled_table(ax, rows, col_labels, col_widths)
            add_footer(fig)
            pdf.savefig(fig)
            plt.close(fig)

        # ------------------------------------------------------------------ #
        # Страница 6: Per-endpoint сравнение задержек (горизонтальный бар)   #
        # ------------------------------------------------------------------ #
        if has_tags:
            fig, (axL, axR) = plt.subplots(1, 2, figsize=(11.69, 8.27))
            fig.subplots_adjust(top=0.86, bottom=0.12, left=0.23, right=0.97, wspace=0.32)
            page_header(fig, "Сравнение задержек по endpoint", args.base_url)

            tag_colors_map = {tag: TAG_COLORS[i % len(TAG_COLORS)] for i, tag in enumerate(tags)}

            # Левый: p95 латентность
            p95_vals = []
            p50_vals = []
            for tag in tags:
                tp = stats.tag_percentiles_ms(tag)
                p95_vals.append(tp.get("p95", 0) if tp else 0)
                p50_vals.append(tp.get("p50", 0) if tp else 0)

            y_pos = range(len(tags))
            colors_bar = [tag_colors_map[t] for t in tags]

            bars95 = axL.barh(list(y_pos), p95_vals, color=colors_bar, alpha=0.85, height=0.6)
            axL.set_yticks(list(y_pos))
            axL.set_yticklabels(tags, fontsize=7.5)
            axL.set_xlabel("задержка, мс")
            axL.set_title("p95 задержка по endpoint", loc="left", fontweight="bold", color=RED)
            axL.bar_label(bars95, labels=[f"{v:.0f}" for v in p95_vals],
                          padding=4, fontsize=8)
            axL.margins(x=0.15)
            style_ax(axL)

            # Правый: RPS по endpoint
            rps_vals = [stats.tag_total_requests(t) / total_time if total_time else 0
                        for t in tags]
            bars_rps = axR.barh(list(y_pos), rps_vals, color=colors_bar, alpha=0.85, height=0.6)
            axR.set_yticks(list(y_pos))
            axR.set_yticklabels([])
            axR.set_xlabel("запросов/с")
            axR.set_title("Средний RPS по endpoint", loc="left", fontweight="bold", color=GREEN)
            axR.bar_label(bars_rps, labels=[f"{v:.1f}" for v in rps_vals],
                          padding=4, fontsize=8)
            axR.margins(x=0.15)
            style_ax(axR)

            add_footer(fig)
            pdf.savefig(fig)
            plt.close(fig)

        # ------------------------------------------------------------------ #
        # Страница 7: RPS по endpoint во времени                             #
        # ------------------------------------------------------------------ #
        if has_tags and has_hist and len(hist) >= 2:
            fig, axes = plt.subplots(1, 2, figsize=(11.69, 8.27))
            fig.subplots_adjust(top=0.86, bottom=0.12, left=0.06, right=0.97, wspace=0.25)
            page_header(fig, "RPS по endpoint во времени", args.base_url)

            # Вычисляем интервальный RPS на тег: diff соседних точек tag_totals
            ts_rps: list[float] = []
            tag_rps_series: dict[str, list[float]] = {tag: [] for tag in tags}
            prev_h = hist[0]
            for h in hist[1:]:
                dt = h["t"] - prev_h["t"]
                if dt > 0:
                    ts_rps.append(h["t"])
                    for tag in tags:
                        d = h["tag_totals"].get(tag, 0) - prev_h["tag_totals"].get(tag, 0)
                        tag_rps_series[tag].append(d / dt)
                prev_h = h

            ax_rps = axes[0]
            for i, tag in enumerate(tags):
                color = TAG_COLORS[i % len(TAG_COLORS)]
                series = tag_rps_series[tag]
                ax_rps.plot(ts_rps, series, label=tag, color=color, linewidth=1.5, alpha=0.85)
            ax_rps.set_title("RPS по endpoint", loc="left", fontweight="bold", color="#1e293b")
            ax_rps.set_xlabel("время, с")
            ax_rps.set_ylabel("запросов/с")
            ax_rps.legend(fontsize=7.5, frameon=False, ncol=1)
            style_ax(ax_rps)

            # Правый: накопительный процент успешных по тегу
            ax_pct = axes[1]
            for i, tag in enumerate(tags):
                color = TAG_COLORS[i % len(TAG_COLORS)]
                ts_all = [h["t"] for h in hist]
                ok_series = []
                for h in hist:
                    n = h["tag_totals"].get(tag, 0)
                    ok = stats.tag_success.get(tag, 0)
                    # Приближение: пропорциональное распределение OK по истории
                    ok_series.append(ok / stats.tag_total_requests(tag) * n
                                     if stats.tag_total_requests(tag) > 0 else 0)
                ax_pct.plot(ts_all, ok_series, label=tag, color=color, linewidth=1.5, alpha=0.85)
            ax_pct.set_title("Успешных запросов накопительно по endpoint",
                             loc="left", fontweight="bold", color=GREEN)
            ax_pct.set_xlabel("время, с")
            ax_pct.set_ylabel("шт.")
            ax_pct.legend(fontsize=7.5, frameon=False, ncol=1)
            style_ax(ax_pct)

            add_footer(fig)
            pdf.savefig(fig)
            plt.close(fig)

        d = pdf.infodict()
        d["Title"] = "Отчёт о нагрузочном тестировании (replay)"
        d["Subject"] = args.base_url
        d["CreationDate"] = started_at

    print(f"  PDF-отчёт         : {path}")


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main() -> None:
    default_file = "tests/curse_breaker_usn.filtered"
    p = argparse.ArgumentParser(
        description="Нагрузочное тестирование бэкенда по JSONL-файлу с реальными данными.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    p.add_argument("--base-url", required=True,
                   help="Базовый URL бэкенда, например http://10.0.8.49:7777")
    p.add_argument("--api-key", required=True,
                   help="Значение заголовка x-api-key")
    p.add_argument("--file", default=default_file,
                   help="Путь к JSONL-файлу с данными")
    p.add_argument("--connections", "-c", type=int, default=50,
                   help="Максимальное число параллельных соединений")
    p.add_argument("--duration", "-d", type=float, default=1.0,
                   help="Время работы в минутах")
    p.add_argument("--ramp", type=float, default=10.0,
                   help="За сколько секунд нарастить соединения до максимума")
    p.add_argument("--timeout", "-t", type=float, default=30.0,
                   help="Таймаут одного запроса, секунд")
    p.add_argument("--tag", action="append", default=None, metavar="TAG",
                   help="Тестировать только этот Tag (повторяемый флаг)")
    p.add_argument("--exclude-tag", action="append", default=None, metavar="TAG",
                   help="Исключить Tag из теста (повторяемый флаг)")
    p.add_argument("--sequential", "-s", action="store_true",
                   help="Отправлять запросы по порядку файла (не случайно); "
                        "после последней записи начинает сначала")
    p.add_argument("--report", default=None,
                   help="Путь к PDF-отчёту (по умолчанию loadtest_report_<дата>.pdf)")
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
