#!/usr/bin/env bash
# Quick Reference: Distributed Task Queue Engine Commands

## SETUP (Already Complete)
cd c:\Users\jaini\OneDrive\Desktop\Proj\distributed-task-queue
python -m venv venv
.\venv\Scripts\Activate.ps1
pip install -r requirements.txt

## TESTING (LOCAL)
# Run all broker tests (no Redis required)
pytest tests/test_broker.py -v

# Run single test
pytest tests/test_broker.py::test_enqueue_high_priority -v

# Run with coverage
pytest --cov=task_queue tests/

# API endpoint validation
python test_api.py

## LOCAL DEPLOYMENT (Needs Redis running separately)
# Terminal 1: Start Redis
redis-server

# Terminal 2: Start workers + API
python main.py
# Runs on http://localhost:8000

## DOCKER DEPLOYMENT (Recommended for Production)
# Full stack with Redis
docker-compose up --build

# Rebuild without cache
docker-compose up --build --force-recreate

# Run in background
docker-compose up -d

# View logs
docker-compose logs -f
docker-compose logs -f app
docker-compose logs -f redis

# Stop everything
docker-compose down

# Clean everything including volumes
docker-compose down -v

## API USAGE EXAMPLES

# 1. HEALTH CHECK
curl http://localhost:8000/health

# 2. ENQUEUE JOB (High Priority)
RESPONSE=$(curl -X POST http://localhost:8000/enqueue \
  -H "Content-Type: application/json" \
  -d '{
    "payload": {"example": "task"},
    "priority": "high",
    "timeout_seconds": 30
  }')
echo $RESPONSE
# Extract job_id: {"job_id": "...", "status": "pending", "priority": "high"}

# 3. CHECK JOB STATUS
curl http://localhost:8000/jobs/YOUR_JOB_ID_HERE

# 4. GET QUEUE STATISTICS
curl http://localhost:8000/queue/stats
# Response: {"queues": {"high": 2, "medium": 1, "low": 0}, "dlq": 0}

# 5. RETRY FAILED JOB (From DLQ)
curl -X POST http://localhost:8000/jobs/JOB_ID/retry

## BATCH JOB SUBMISSION
# Submit 10 jobs rapidly to test worker pool
for i in {1..10}; do
  curl -X POST http://localhost:8000/enqueue \
    -H "Content-Type: application/json" \
    -d '{"payload": {"task_number": '$i'}, "priority": "medium"}'
  echo "Job $i submitted"
done

## TESTING FAILURE & RETRY
# Job that will fail and retry
curl -X POST http://localhost:8000/enqueue \
  -H "Content-Type: application/json" \
  -d '{"payload": {"fail": true}, "priority": "low", "max_retries": 3}'

# Watch logs to see retry backoff (1s, 2s, 4s)
docker-compose logs -f app | grep -i retry

## MONITORING & DEBUGGING
# Watch all events in real-time
docker-compose logs -f

# Check specific component
docker-compose logs -f app   # Application logs
docker-compose logs redis    # Redis logs only

# Interactive shell into container
docker-compose exec app /bin/bash
pytest -v            # Run tests inside container

# View running containers
docker-compose ps

# Execute command in container
docker-compose exec app python -c "from task_queue.broker import queue_stats; print(queue_stats())"

## TROUBLESHOOTING
# If Docker port conflicts
docker-compose down -v
docker system prune -a --volumes

# If Redis connection fails
docker-compose restart redis

# Check if ports are in use
netstat -an | grep 8000
netstat -an | grep 6379

# Rebuild clean state
rm -rf .pytest_cache __pycache__ .docker
docker-compose build --no-cache
docker-compose up

## DEVELOPMENT
# View file structure
tree . -I 'venv|__pycache__|*.pyc'

# Code quality checks
pytest tests/test_broker.py -v --tb=short
isort task_queue/ api/          # Fix imports
black task_queue/ api/ tests/   # Format code

# Type checking
mypy task_queue/ api/           # If mypy installed
