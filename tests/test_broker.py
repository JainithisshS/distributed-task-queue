"""
Test suite for Redis broker module.

Tests job enqueueing, dequeueing, status updates, and dead letter queue
operations using a fake Redis instance.
"""

import pytest
import fakeredis

from task_queue.broker import (
    QUEUE_HIGH,
    QUEUE_MEDIUM,
    QUEUE_LOW,
    DLQ_KEY,
    JOB_PREFIX,
    enqueue,
    dequeue,
    update_status,
    get_job,
    send_to_dlq,
    queue_stats,
    requeue_from_dlq,
    r,
)
from task_queue.job import Job
import task_queue.broker as broker_module


@pytest.fixture
def fake_redis():
    """
    Fixture that patches Redis connection with a fake instance.
    """
    fake_r = fakeredis.FakeRedis(decode_responses=True)
    broker_module.r = fake_r
    yield fake_r
    # Clean up after test
    fake_r.flushall()


def test_enqueue_high_priority(fake_redis):
    """
    Test enqueuing a job with high priority.
    
    Verifies that the job is placed in the high priority queue
    and not in medium or low queues.
    """
    job = Job(priority="high", payload={"test": "data"})
    enqueue(job)
    
    assert fake_redis.llen(QUEUE_HIGH) == 1
    assert fake_redis.llen(QUEUE_MEDIUM) == 0
    assert fake_redis.llen(QUEUE_LOW) == 0


def test_enqueue_routes_by_priority(fake_redis):
    """
    Test that jobs are routed to correct priority queues.
    """
    high_job = Job(priority="high", payload={"priority": "high"})
    medium_job = Job(priority="medium", payload={"priority": "medium"})
    low_job = Job(priority="low", payload={"priority": "low"})
    
    enqueue(high_job)
    enqueue(medium_job)
    enqueue(low_job)
    
    assert fake_redis.llen(QUEUE_HIGH) == 1
    assert fake_redis.llen(QUEUE_MEDIUM) == 1
    assert fake_redis.llen(QUEUE_LOW) == 1


def test_dequeue_respects_priority_order(fake_redis):
    """
    Test that dequeue respects priority order.
    
    Enqueues one low and one high priority job, then dequeues twice.
    Verifies high priority job is returned first.
    """
    low_job = Job(priority="low", payload={"priority": "low"})
    high_job = Job(priority="high", payload={"priority": "high"})
    
    enqueue(low_job)
    enqueue(high_job)
    
    # First dequeue should get high priority job
    first = dequeue(timeout=1)
    assert first is not None
    assert first.priority == "high"
    
    # Second dequeue should get low priority job
    second = dequeue(timeout=1)
    assert second is not None
    assert second.priority == "low"


def test_update_status_persists(fake_redis):
    """
    Test that updating job status persists to Redis.
    """
    job = Job(payload={"test": "data"})
    enqueue(job)
    
    # Fetch job
    fetched = get_job(job.id)
    assert fetched.status == "pending"
    
    # Update status
    fetched.status = "running"
    update_status(fetched)
    
    # Fetch again and verify
    refetched = get_job(job.id)
    assert refetched.status == "running"


def test_send_to_dlq(fake_redis):
    """
    Test sending a job to the dead letter queue.
    """
    job = Job(payload={"test": "data"})
    send_to_dlq(job)
    
    assert fake_redis.llen(DLQ_KEY) == 1


def test_requeue_from_dlq_success(fake_redis):
    """
    Test successfully requeuing a job from DLQ.
    """
    job = Job(priority="medium", payload={"test": "data"})
    send_to_dlq(job)
    
    # Verify job is in DLQ
    assert fake_redis.llen(DLQ_KEY) == 1
    
    # Requeue the job
    success = requeue_from_dlq(job.id)
    
    assert success is True
    assert fake_redis.llen(DLQ_KEY) == 0
    assert fake_redis.llen(QUEUE_MEDIUM) == 1


def test_requeue_from_dlq_not_found(fake_redis):
    """
    Test requeuing a job that doesn't exist in DLQ.
    """
    success = requeue_from_dlq("nonexistent-id")
    assert success is False


def test_queue_stats(fake_redis):
    """
    Test queue statistics retrieval.
    """
    high_job = Job(priority="high", payload={"priority": "high"})
    medium_job = Job(priority="medium", payload={"priority": "medium"})
    low_job = Job(priority="low", payload={"priority": "low"})
    
    enqueue(high_job)
    enqueue(medium_job)
    enqueue(low_job)
    
    failed_job = Job(payload={"test": "data"})
    send_to_dlq(failed_job)
    
    stats = queue_stats()
    
    assert stats["high"] == 1
    assert stats["medium"] == 1
    assert stats["low"] == 1
    assert stats["dlq"] == 1


def test_get_job_not_found(fake_redis):
    """
    Test retrieving a non-existent job returns None.
    """
    job = get_job("nonexistent-id")
    assert job is None
