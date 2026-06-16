# loadtest_replay.py — Руководство по использованию

Нагрузочный тест по реальным данным. Считывает JSONL-файл с историческими вызовами бэкенда, строит пул запросов и в N параллельных соединений непрерывно отправляет запросы из этого пула. Поддерживает два режима: **случайный** (по умолчанию, `random.choice`) и **последовательный** (`--sequential` — строго по порядку файла, после последней записи начинает сначала). Замеряет RPS, латентность и коды ответов — суммарно и отдельно по каждому endpoint.

---

## Зависимости

```bash
pip install aiohttp matplotlib
```

---

## Формат входного файла

Каждая строка файла — JSON-объект:

```json
{
  "Tag":         "GetUser",
  "ClientID":    "4a8379f7-d924-4064-80bf-974d647de0f2",
  "Inn":         "744725130545",
  "TaxRate":     6,
  "TaxSystem":   "usn_d",
  "StartYear":   2026,
  "RequestID":   "76cece8a-8a97-49e3-8bcb-13c6c8006af5",
  "OperationID": "cbb6e2de-ce9f-...",
  "SourceID":    "000054078",
  "FlowID":      "4f01b975-7d5d-..."
}
```

| Поле | Куда идёт |
|------|-----------|
| `Tag` | Определяет endpoint |
| `ClientID` | Заголовок `x-client-id` |
| `RequestID` | Path-параметр `{RequestID}` → `/sources/{RequestID}/state` |
| `OperationID` | Path-параметр `{OperationID}` → `/operations/{OperationID}` |
| `FlowID` | Path-параметр `{FlowID}` → `/fns_reports/flows/{FlowID}/detailed` |
| `Inn`, `TaxRate`, `TaxSystem`, `StartYear` | JSON-body для POST `/operations` |

### Поддерживаемые значения Tag

| Tag | Метод | Путь |
|-----|-------|------|
| `GetUser` | GET | `/users` |
| `GetSourcesInfo` | GET | `/sources` |
| `GetOperations` | POST | `/operations` |
| `GetTasks` | GET | `/tasks` |
| `ListCompletedTasks` | GET | `/tasks/completed` |
| `GetTaxLimits` | GET | `/references/tax_limits` |
| `GetOperationById` | GET | `/operations/{OperationID}` |
| `GetSourceState` | GET | `/sources/{RequestID}/state` |
| `GetFnsFlowFullInfo` | GET | `/fns_reports/flows/{FlowID}/detailed` |

---

## Параметры командной строки

| Параметр | Короткий | По умолчанию | Описание |
|----------|----------|--------------|----------|
| `--base-url` | — | **обязательный** | Базовый URL бэкенда |
| `--api-key` | — | **обязательный** | Значение заголовка `x-api-key` |
| `--file` | — | `tests/curse_breaker_usn.filtered` | Путь к JSONL-файлу |
| `--connections` | `-c` | `50` | Максимальное число параллельных соединений |
| `--duration` | `-d` | `1.0` | Время теста в минутах |
| `--ramp` | — | `10.0` | Время нарастания соединений до максимума (секунды) |
| `--timeout` | `-t` | `30.0` | Таймаут одного запроса (секунды) |
| `--tag` | — | все | Тестировать только указанный Tag (повторяемый) |
| `--exclude-tag` | — | — | Исключить Tag из теста (повторяемый) |
| `--sequential` | `-s` | выкл | Отправлять запросы по порядку файла, а не случайно |
| `--report` | — | авто | Путь к PDF-отчёту |
| `--no-report` | — | — | Не создавать PDF-отчёт |

---

## Сценарии тестирования

### 1. Full replay — полная смешанная нагрузка (рекомендуется)

Все 9 endpoint-ов вместе. Реалистичная нагрузка: соотношение запросов к разным endpoint-ам соответствует реальному трафику (в файле каждый Tag представлен одинаковым числом записей — 397 уникальных «сессий»).

```bash
python3 loadtest_replay.py \
  --base-url http://10.0.8.49:7777 \
  --api-key bee03ffd-3c6f-4e88-8b7b-b4ac673c9cec \
  --connections 100 \
  --duration 5
```

**Что даёт:** общий RPS системы, суммарная доля ошибок, выявление узких мест через разбивку по endpoint в отчёте.

---

### 2. Изоляция endpoint — найти узкое место

Тестировать один endpoint с максимальной нагрузкой.

```bash
# Самый тяжёлый endpoint:
python3 loadtest_replay.py \
  --base-url http://10.0.8.49:7777 \
  --api-key bee03ffd-3c6f-4e88-8b7b-b4ac673c9cec \
  --connections 200 --duration 3 \
  --tag GetFnsFlowFullInfo

# POST /operations:
python3 loadtest_replay.py \
  --base-url http://10.0.8.49:7777 \
  --api-key bee03ffd-3c6f-4e88-8b7b-b4ac673c9cec \
  --connections 100 --duration 3 \
  --tag GetOperations

# Несколько тегов одновременно:
python3 loadtest_replay.py \
  --base-url http://10.0.8.49:7777 \
  --api-key bee03ffd-3c6f-4e88-8b7b-b4ac673c9cec \
  --connections 100 --duration 3 \
  --tag GetOperationById --tag GetFnsFlowFullInfo
```

**Что даёт:** предельный RPS и p95-латентность конкретного endpoint-а без «конкуренции» с остальными.

---

### 3. Только чтение — безопасный тест без побочных эффектов

Исключает POST `/operations`, который может изменять данные.

```bash
python3 loadtest_replay.py \
  --base-url http://10.0.8.49:7777 \
  --api-key bee03ffd-3c6f-4e88-8b7b-b4ac673c9cec \
  --connections 150 --duration 5 \
  --exclude-tag GetOperations
```

**Что даёт:** нагрузка на все READ endpoint-ы без риска побочных эффектов. Подходит для тестирования в боевой среде.

---

### 4. Последовательный проход по файлу (`--sequential`)

Все 3 573 записи отправляются строго по порядку. Несколько соединений берут из очереди следующую запись одновременно, не дублируя друг друга. После последней записи файл начинается заново. В итоге каждый endpoint получает запросы ровно в той последовательности, в которой они зафиксированы в файле.

```bash
python3 loadtest_replay.py \
  --base-url http://10.0.8.49:7777 \
  --api-key bee03ffd-3c6f-4e88-8b7b-b4ac673c9cec \
  --connections 20 --duration 5 --sequential
```

**Что даёт:** воспроизведение реального сценария пользовательской сессии (GetUser → GetSourcesInfo → GetOperations → … для одного ClientID), а не случайного микса. В итоге — покрытие всех уникальных пользователей и значений. В конце теста выводится сколько полных проходов по файлу выполнено.

Можно комбинировать с `--tag`, чтобы пройти по порядку только по одному endpoint-у:

```bash
python3 loadtest_replay.py \
  --base-url http://10.0.8.49:7777 \
  --api-key bee03ffd-3c6f-4e88-8b7b-b4ac673c9cec \
  --connections 10 --duration 2 --sequential --tag GetOperationById
```

---

### 5. Базовые показатели (scan mode)

Один быстрый прогон при низкой нагрузке — получить «нулевые» p95 для сравнения.

```bash
python3 loadtest_replay.py \
  --base-url http://10.0.8.49:7777 \
  --api-key bee03ffd-3c6f-4e88-8b7b-b4ac673c9cec \
  --connections 5 --duration 0.5 --ramp 2
```

**Что даёт:** базовую латентность при минимальной нагрузке (холодный старт vs нагруженный бэкенд).

---

## Структура PDF-отчёта (7 страниц)

| Страница | Содержание |
|----------|------------|
| 1 | **Сводка**: KPI-карточки (всего запросов, успешных %, avg RPS, p95), вердикт, таблицы параметров и итогов |
| 2 | **Динамика**: соединения, RPS, накопительные OK/FAIL/ERR, средняя задержка — всё во времени |
| 3 | **Ошибки**: интенсивность ошибок/с, доля ошибок за интервал, накопительные FAIL/ERR |
| 4 | **Распределение**: HTTP-коды (bar chart), гистограмма задержек с маркером p95 |
| 5 | **Per-endpoint таблица**: для каждого Tag — запросов, OK, FAIL, ERR, RPS, p50/p95/p99 мс |
| 6 | **Сравнение**: горизонтальные бар-чарты p95-латентности и RPS по каждому endpoint |
| 7 | **Временные ряды**: RPS по endpoint во времени + накопительные успешные запросы по endpoint |

### Как читать отчёт

- **Страница 5** — главная для выявления узкого места: сортируйте по p95, смотрите на долю FAIL/ERR.
- **Страница 6** (левый чарт) — быстрый визуальный ответ на вопрос «какой endpoint самый медленный?»
- **Страница 7** — если один endpoint деградирует со временем, его RPS-линия «просядет» при стабильных остальных.
- **Страница 3** — всплески ошибок в секунду помогают найти момент деградации (например, исчерпание пула соединений в БД).

---

## Рекомендуемый порядок тестирования

```
1. Scan mode (5 соединений) → получить базовые p50/p95
2. Full replay (50-100 соединений, 3-5 минут) → реалистичная картина под случайной нагрузкой
3. Sequential (20-50 соединений) → воспроизвести реальные сессии по порядку файла
4. Изоляция тяжёлых endpoint-ов (GetFnsFlowFullInfo, GetOperations) с высокой нагрузкой
5. Стресс-тест до деградации → увеличивать --connections до роста ERR > 1%
```

---

## Проверка без реального бэкенда

Для smoke-теста используйте `testserver.py` (он отвечает на `/users`):

```bash
# Терминал 1: запустить тестовый сервер
python3 testserver.py --port 7777 --api-key testkey

# Терминал 2: запустить replay только с GetUser
python3 loadtest_replay.py \
  --base-url http://127.0.0.1:7777 \
  --api-key testkey \
  --connections 10 --duration 0.2 --ramp 2 \
  --tag GetUser --no-report
```

---

## Отличия от loadtest.py

| | loadtest.py | loadtest_replay.py |
|--|-------------|-------------------|
| URL | фиксированный `--url` | из файла по Tag |
| Метод | `--method` | из TAG_MAP |
| Заголовки | `--header "Key: Value"` | x-client-id из файла, x-api-key из CLI |
| Тело | `--body` | JSON из полей записи (для POST /operations) |
| Порядок запросов | фиксированный URL | случайный или `--sequential` (порядок файла) |
| Отчёт | 4 страницы | 7 страниц (+ per-endpoint разбивка) |
| Консоль | суммарно | суммарно + таблица по endpoint-ам |
