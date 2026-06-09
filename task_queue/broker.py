"""
Redis broker module for task queue system.

Manages job persistence, queueing by priority, and dead letter queue operations.
All operations are connected to Redis and handle errors gracefully.
"""

import os
from typing import Dict, List, Optional

import redis

from task_queue.job import Job

# Redis queue keys
QUEUE_HIGH = "taskqueue:high"
QUEUE_MEDIUM = "taskqueue:medium"
QUEUE_LOW = "taskqueue:low"
DLQ_KEY = "taskqueue:dlq"
JOB_PREFIX = "job:"
PRIORITY_QUEUES = [QUEUE_HIGH, QUEUE_MEDIUM, QUEUE_LOW]

# Redis connection
REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
r = redis.Redis(host=REDIS_HOST, port=6379, decode_responses=True)


def enqueue(job: Job) -> None:
    """
    Enqueue a job to the appropriate priority queue.
    
    Stores the full job JSON in a Redis hash and pushes the job ID
    to the priority queue list.
    
    Args:
        job: Job object to enqueue
    """
    try:
        # Store job data in Redis hash
        job_key = f"{JOB_PREFIX}{job.id}"
        r.hset(job_key, "data", job.to_json())
        
        # Route to correct priority queue
        if job.priority == "high":
            r.lpush(QUEUE_HIGH, job.id)
        elif job.priority == "low":
            r.lpush(QUEUE_LOW, job.id)
        else:  # medium
            r.lpush(QUEUE_MEDIUM, job.id)
    except redis.RedisError as e:
        raise RuntimeError(f"Failed to enqueue job {job.id}: {e}")


def dequeue(timeout: int = 5) -> Optional[Job]:
    """
    Dequeue a job from priority queues, respecting priority order.
    
    Attempts to dequeue from high priority queue first, then medium, then low.
    Uses blocking operation with specified timeout.
    
    Args:
        timeout: Blocking timeout in seconds
        
    Returns:
        Job object if available, None if timeout occurs
    """
    try:
        result = r.brpop(PRIORITY_QUEUES, timeout=timeout)
        
        if result is None:
            return None
        
        # result is tuple (queue_name, job_id)
        job_id = result[1]
        
        # Retrieve job from hash
        job_key = f"{JOB_PREFIX}{job_id}"
        raw_job = r.hget(job_key, "data")
        
        if raw_job is None:
            return None
        
        return Job.from_json(raw_job)
    except redis.RedisError as e:
        raise RuntimeError(f"Failed to dequeue job: {e}")


def update_status(job: Job) -> None:
    """
    Update a job's status in Redis.
    
    Overwrites the existing job hash entry with the current job state.
    
    Args:
        job: Job object with updated state
    """
    try:
        job_key = f"{JOB_PREFIX}{job.id}"
        r.hset(job_key, "data", job.to_json())
    except redis.RedisError as e:
        raise RuntimeError(f"Failed to update status for job {job.id}: {e}")


def get_job(job_id: str) -> Optional[Job]:
    """
    Retrieve a job by ID from Redis.
    
    Args:
        job_id: Unique job identifier
        
    Returns:
        Job object if found, None otherwise
    """
    try:
        job_key = f"{JOB_PREFIX}{job_id}"
        raw_job = r.hget(job_key, "data")
        
        if raw_job is None:
            return None
        
        return Job.from_json(raw_job)
    except redis.RedisError as e:
        raise RuntimeError(f"Failed to get job {job_id}: {e}")


def send_to_dlq(job: Job) -> None:
    """
    Move a failed job to the dead letter queue.
    
    Args:
        job: Job object that failed
    """
    try:
        r.lpush(DLQ_KEY, job.to_json())
    except redis.RedisError as e:
        raise RuntimeError(f"Failed to send job {job.id} to DLQ: {e}")


def queue_stats() -> Dict[str, int]:
    """
    Get current queue statistics.
    
    Returns:
        Dict with counts for high, medium, low queues and DLQ
    """
    try:
        return {
            "high": r.llen(QUEUE_HIGH),
            "medium": r.llen(QUEUE_MEDIUM),
            "low": r.llen(QUEUE_LOW),
            "dlq": r.llen(DLQ_KEY),
        }
    except redis.RedisError as e:
        raise RuntimeError(f"Failed to get queue stats: {e}")


def requeue_from_dlq(job_id: str) -> bool:
    """
    Attempt to requeue a job from the dead letter queue.
    
    Searches DLQ for a job with matching ID, removes it, resets state,
    and re-enqueues as pending.
    
    Args:
        job_id: ID of job to requeue
        
    Returns:
        True if found and requeued, False if not found
    """
    try:
        # Get all items from DLQ
        dlq_items = r.lrange(DLQ_KEY, 0, -1)
        
        for item in dlq_items:
            job = Job.from_json(item)
            if job.id == job_id:
                # Remove from DLQ
                r.lrem(DLQ_KEY, 1, item)
                
                # Reset job state
                job.retries = 0
                job.status = "pending"
                job.error = None
                
                # Re-enqueue
                enqueue(job)
                return True
        
        return False
    except redis.RedisError as e:
        raise RuntimeError(f"Failed to requeue job from DLQ: {e}")
