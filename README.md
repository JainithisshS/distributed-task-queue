# Distributed Task Queue

![CI](https://github.com/JainithisshS/distributed-task-queue/actions/workflows/ci.yml/badge.svg)

Lightweight distributed task queue engine in Python — FastAPI API, Redis broker, multiprocessing workers, and tests.

Quick start

1. Create virtualenv and install dependencies:

```bash
python -m venv .venv
. .venv/bin/activate
pip install -r requirements.txt
```

2. Run locally (Redis required):

```bash
python main.py
```

3. Run tests:

```bash
pytest -q
```

Containerized:

```bash
docker compose up --build -d
```

License: MIT
