Project Guide — Distributed Task Queue
=====================================

This guide explains the architecture, internal components, configuration, running, testing and maintenance tasks for the Distributed Task Queue project.

1. Architecture Overview
------------------------

- API: `api/` hosts FastAPI routes to enqueue jobs, query status and healthchecks.
- Broker: `task_queue/broker.py` implements enqueue/dequeue, job status updates, statistics, and DLQ handling on top of Redis.
- Job model: `task_queue/job.py` defines `Job` with JSON serialization and lifecycle states.
- Worker: `task_queue/worker.py` runs job execution in separate processes, manages heartbeats and timeouts (SIGALRM in main process, thread fallback in tests).
- Watchdog: `task_queue/watchdog.py` checks for `running` jobs with stale heartbeats and requeues them.
- Logger: `task_queue/logger.py` provides structured JSON logging used across components.

2. Folder layout (top-level)
---------------------------

- `api/` — FastAPI route definitions and app startup bits.
- `task_queue/` — core package (broker, job, worker, watchdog, logger).
- `tools/` — utility scripts (enqueue demo, CLI helpers).
- `tests/` — pytest tests using `fakeredis` and pytest-asyncio.
- `Dockerfile`, `docker-compose.yml` — containerization for app + redis.
- `GUIDE.md`, `README.md` — docs.

3. Configuration
----------------

Environment variables (defaults used in code where appropriate):

- `REDIS_URL` — Redis connection string (default: `redis://localhost:6379/0`).
- `WORKER_COUNT` — number of worker processes to spawn when using `main.py`.

4. Running locally
-------------------

Prerequisites: Python 3.11, Redis (local or docker).

- Start Redis with Docker: `docker run -d --name redis -p 6379:6379 redis:7`.
- Activate venv, install deps: `pip install -r requirements.txt`.
- Start the application (spawns workers and API): `python main.py`.

Tips:
- Use `tools/enqueue_demo.py` to place a sample job.
- Check `app.log` for structured JSON lifecycle logs.

5. Docker / Compose
--------------------

Run:

```bash
docker compose up --build -d
```

This starts Redis and the app container. Use `docker compose logs -f` to watch logs.

6. Testing
----------

Unit tests use `pytest`, `fakeredis`, and `pytest-asyncio`. Run:

```bash
pytest -q
```

Notes about tests:
- SIGALRM can't be used from non-main threads; the worker code implements a fallback (thread join with timeout) to keep tests deterministic.

7. Developer notes / internals
------------------------------

- Job lifecycle: `queued` → `running` → `succeeded` | `failed` → optionally moved to DLQ.
- Heartbeats: workers update a heartbeat key in Redis; the watchdog scans for stale heartbeats and requeues the job.
- Timeouts: in production on Linux, SIGALRM is used for accurate timeouts; tests use thread join fallback.

8. Debugging
------------

- If jobs never run: verify Redis connection, worker processes are running, and broker list has items.
- If tests fail only on Windows/CI: ensure Python 3.11 and `pydantic` wheel compatibility; prefer WSL or Linux runners for CI.

9. Maintenance / next steps
--------------------------

- Remove any committed `venv` directories and large files from the repository (use `.gitignore` and `git rm --cached` before committing) — these inflate repo size.
- Ensure GitHub Actions run and pass — add badge to `README.md` (already added) and enable branch protection.
- Add examples and a short API reference (OpenAPI is available at `/docs` when the app runs).

10. Project completion assessment
---------------------------------

Based on the current repository contents and prior verification runs:

- Implemented and working:
  - Core queue engine (`task_queue` package)
  - FastAPI endpoints and demo enqueue script
  - Worker model with heartbeat and timeout handling
  - Watchdog for recovering stuck jobs
  - Unit tests covering broker/worker behavior
  - Docker Compose for app + Redis

- Remaining or optional improvements:
  - Remove committed virtualenvs (cleanup) — actionable and recommended
  - Confirm GitHub remote receives pushes and Actions execute successfully (requires authentication)
  - Add more integration tests (end-to-end with real Redis and multiple workers)
  - Add monitoring/metrics export (Prometheus) and graceful shutdown handling across containers

Conclusion: Functionally, the project is effectively complete for a production-grade prototype: core features, tests, and containers exist and have been validated locally. A small amount of housekeeping (cleanup of venvs, CI verification on GitHub, and optional monitoring) remains to make the repo production-ready.

11. Contact / support
---------------------

If you want, I can:
- Remove `venv` directories from the repository and rewrite history if necessary (or just remove and commit)
- Help configure `gh` auth or PAT and push changes and enable branch protection
- Add example OpenAPI usage and cURL snippets for common flows
