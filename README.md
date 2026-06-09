# Distributed Task Queue

![CI](https://github.com/JainithisshS/distributed-task-queue/actions/workflows/ci.yml/badge.svg)
![Python](https://img.shields.io/badge/python-3.11-blue.svg)
![License](https://img.shields.io/badge/license-MIT-green.svg)

A **distributed task queue engine built from scratch** in Python — no Celery, no external framework. Jobs are enqueued via a FastAPI REST API, persisted and routed by a Redis broker across three priority levels, and executed by a pool of multiprocessing workers with automatic retry, timeout protection, and dead-letter handling.

---

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                        Clients                              │
│              (curl / HTTP / enqueue_demo.py)                │
└────────────────────────┬────────────────────────────────────┘
                         │ HTTP
                         ▼
┌─────────────────────────────────────────────────────────────┐
│                    FastAPI Layer  (api/)                     │
│  POST /enqueue  GET /jobs/{id}  GET /queue/stats            │
│  POST /jobs/{id}/retry          GET /health                 │
└────────────────────────┬────────────────────────────────────┘
                         │ enqueue / get_job / requeue
                         ▼
┌─────────────────────────────────────────────────────────────┐
│                  Redis Broker  (broker.py)                  │
│                                                             │
│   taskqueue:high  ──┐                                       │
│   taskqueue:medium ─┼──► BRPOP (priority order) ──► Worker │
│   taskqueue:low   ──┘                                       │
│                                                             │
│   taskqueue:dlq  ◄── failed jobs (retries exhausted)        │
│   job:<id>  hash ◄── full job JSON + status                 │
└────────────────────────┬────────────────────────────────────┘
                         │ dequeue / update_status / send_to_dlq
                         ▼
┌─────────────────────────────────────────────────────────────┐
│              Worker Pool  (worker.py + main.py)             │
│                                                             │
│   Process 0 ─┐                                              │
│   Process 1 ─┼──► run_worker() loop                         │
│   Process 2 ─┤     • dequeue (BRPOP, 5 s timeout)          │
│   Process 3 ─┘     • execute with SIGALRM timeout           │
│                     • exponential backoff retry (2^n s)     │
│                     • heartbeat every 5 s → Redis key TTL   │
└────────────────────────┬────────────────────────────────────┘
                         │ stale heartbeat detection
                         ▼
┌─────────────────────────────────────────────────────────────┐
│                  Watchdog  (watchdog.py)                    │
│  Scans "running" jobs every 15 s, requeues orphans          │
└─────────────────────────────────────────────────────────────┘
```

---

## Components

| Module | File | Responsibility |
|---|---|---|
| **Job model** | `task_queue/job.py` | `Job` dataclass — UUID generation, priority, status, retry count, JSON serialisation |
| **Broker** | `task_queue/broker.py` | `enqueue`, `dequeue`, `update_status`, `get_job`, `send_to_dlq`, `requeue_from_dlq`, `queue_stats` |
| **Worker** | `task_queue/worker.py` | Blocking dequeue loop, SIGALRM-based timeout, exponential backoff retry, heartbeat writes |
| **Watchdog** | `task_queue/watchdog.py` | 15 s scan cycle, TTL-based stale heartbeat detection, automatic re-enqueue of orphaned jobs |
| **API** | `api/routes.py` | FastAPI app — five REST endpoints, Pydantic request/response models |
| **Logger** | `task_queue/logger.py` | Structured JSON logger — `timestamp`, `level`, `worker_id`, `job_id`, `status`, `duration_ms` |
| **Entry point** | `main.py` | Spawns worker pool + watchdog as child processes, starts Uvicorn in main process |

---

## Design Decisions

**Why Redis lists instead of Redis Streams?**
Redis `BRPOP` across multiple list keys gives immediate priority-aware blocking pop in a single command without consumer groups or acknowledgement complexity. The trade-off (no persistent consumer group replay) is acceptable for a prototype; upgrading to Streams is a one-file change in `broker.py`.

**Why SIGALRM for timeouts?**
`SIGALRM` is the only Python-native, zero-overhead way to interrupt an arbitrary synchronous callable at the OS level. `threading.Timer` cannot forcibly interrupt a CPU-bound job. Workers run on Linux (production) where `SIGALRM` is available; tests running in non-main threads automatically fall back to `thread.join(timeout)`.

**Why multiprocessing over asyncio workers?**
Task payloads are arbitrary Python callables that may be CPU-bound. `asyncio` would serialize them on one event loop thread. Separate OS processes give true parallelism and hard fault isolation — a segfaulting job cannot bring down the API server.

**Why exponential backoff capped at 3 retries?**
Retry delay doubles per attempt (1 s → 2 s → 4 s). Beyond 3 failures the job moves to the DLQ for human inspection rather than retrying indefinitely and blocking the queue.

---

## Quick Start (local)

**Prerequisites:** Python 3.11, Docker (for Redis)

```bash
# 1. Clone and set up environment
git clone https://github.com/JainithisshS/distributed-task-queue.git
cd distributed-task-queue
python3.11 -m venv .venv
source .venv/bin/activate          # Windows: .venv\Scripts\activate
pip install -r requirements.txt

# 2. Start Redis
docker run -d --name redis -p 6379:6379 redis:7

# 3. Start workers + API
python main.py
# API is live at http://localhost:8000
# Logs stream to app.log as structured JSON
```

---

## Docker Compose (recommended)

Brings up Redis + the app in one command:

```bash
docker compose up --build -d

# Tail logs
docker compose logs -f app

# Stop everything
docker compose down
```

---

## API Reference

### `GET /health`
```bash
curl http://localhost:8000/health
```
```json
{"status": "ok"}
```

---

### `POST /enqueue` — Submit a job
```bash
curl -X POST http://localhost:8000/enqueue \
  -H "Content-Type: application/json" \
  -d '{
    "payload": {"task": "send_invoice", "user_id": 42},
    "priority": "high",
    "timeout_seconds": 30,
    "max_retries": 3
  }'
```
```json
{
  "job_id": "d3f1a2b4-...",
  "status": "pending",
  "priority": "high"
}
```

**Priority values:** `high` | `medium` | `low`

---

### `GET /jobs/{job_id}` — Poll job status
```bash
curl http://localhost:8000/jobs/d3f1a2b4-...
```
```json
{
  "job_id": "d3f1a2b4-...",
  "status": "done",
  "priority": "high",
  "retries": 0,
  "created_at": "2024-01-15T10:30:00Z",
  "updated_at": "2024-01-15T10:30:02Z"
}
```

**Status flow:** `pending` → `running` → `done` | `failed` → *(DLQ)*

---

### `GET /queue/stats` — Queue depths
```bash
curl http://localhost:8000/queue/stats
```
```json
{
  "queues": {"high": 2, "medium": 0, "low": 5},
  "dlq": 1
}
```

---

### `POST /jobs/{job_id}/retry` — Requeue from DLQ
```bash
curl -X POST http://localhost:8000/jobs/d3f1a2b4-.../retry
```
```json
{"requeued": true}
```

---

## Running Tests

Tests use `fakeredis` — no running Redis required:

```bash
pytest -v
```

Expected output:
```
tests/test_broker.py::test_enqueue_high_priority              PASSED
tests/test_broker.py::test_enqueue_routes_by_priority         PASSED
tests/test_broker.py::test_dequeue_respects_priority_order    PASSED
tests/test_broker.py::test_update_status_persists             PASSED
tests/test_broker.py::test_send_to_dlq                        PASSED
tests/test_broker.py::test_requeue_from_dlq_success           PASSED
tests/test_broker.py::test_requeue_from_dlq_not_found         PASSED
tests/test_broker.py::test_queue_stats                        PASSED
tests/test_broker.py::test_get_job_not_found                  PASSED
tests/test_worker.py::test_successful_job_sets_status_done    PASSED
tests/test_worker.py::test_failing_job_retries_three_times    PASSED
tests/test_worker.py::test_failed_job_lands_in_dlq            PASSED
```

Run with coverage:
```bash
pytest --cov=task_queue --cov-report=term-missing tests/
```

---

## Benchmarks

Measured on a local machine using `tools/benchmark.py` (fakeredis, in-process — pure broker overhead, no network).

| Metric | Result |
|---|---|
| Enqueue throughput | **~2,400 jobs/sec** (single-threaded) |
| Dequeue throughput | **~3,800 jobs/sec** (single-threaded) |
| Concurrent enqueue throughput | **~1,700 jobs/sec** (4 threads, 10K jobs) |
| Round-trip broker latency p50 | **< 1 ms** |
| Round-trip broker latency p99 | **< 1 ms** |
| Priority ordering correctness | **500/500 high-priority jobs** drained before first low-priority job |
| Concurrent errors | **0** across 10,000 jobs on 4 threads |

Run the benchmark yourself:
```bash
python tools/benchmark.py
```

> Numbers represent broker layer performance (Redis enqueue/dequeue). End-to-end job throughput is bounded by worker count and task execution time.

---

## Configuration

| Environment Variable | Default | Description |
|---|---|---|
| `REDIS_HOST` | `localhost` | Redis hostname |
| `REDIS_PORT` | `6379` | Redis port |
| `NUM_WORKERS` | `4` | Worker process count (set in `main.py`) |

---

## Project Layout

```
distributed-task-queue/
├── task_queue/          # Core engine
│   ├── job.py           # Job dataclass + serialisation
│   ├── broker.py        # Redis broker (enqueue/dequeue/DLQ)
│   ├── worker.py        # Worker loop + timeout + retry
│   ├── watchdog.py      # Heartbeat monitor + orphan recovery
│   └── logger.py        # Structured JSON logger
├── api/
│   └── routes.py        # FastAPI endpoints
├── tests/
│   ├── test_broker.py           # Broker unit tests (fakeredis)
│   ├── test_worker.py           # Worker unit tests
│   └── test_api_integration.py  # End-to-end API tests
├── tools/
│   ├── enqueue_demo.py     # Demo job producer
│   ├── QUICK_REFERENCE.sh  # Common commands cheatsheet
│   └── start_bg.sh         # Run app in background (Linux)
├── .github/workflows/
│   └── ci.yml           # GitHub Actions CI
├── main.py              # Entry point — spawns workers + API
├── Dockerfile
├── docker-compose.yml
└── requirements.txt
```

---

## License

MIT
