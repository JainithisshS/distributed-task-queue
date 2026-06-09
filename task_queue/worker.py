"""
Worker module for processing jobs from the task queue.

Implements job execution with timeout handling, retry logic, and
worker health heartbeat reporting.
"""

import random
import signal
import threading
import time
from typing import Optional

import redis

from task_queue.broker import (
    DLQ_KEY,
    QUEUE_HIGH,
    QUEUE_MEDIUM,
    QUEUE_LOW,
    enqueue,
    dequeue,
    update_status,
    send_to_dlq,
    r,
)
from task_queue.job import Job
from task_queue.logger import get_logger


def process_job(job: Job) -> None:
    """
    Execute the actual task logic for a job.
    
    For demonstration, simulates work by sleeping. In production,
    this would call the actual user-defined task function.
    
    Args:
        job: Job object to process
        
    Raises:
        Exception: If job payload contains fail=True (for testing)
    """
    logger = get_logger(f"process_job")
    
    # Simulate work
    sleep_duration = random.uniform(0.5, 2.0)
    time.sleep(sleep_duration)
    
    # Simulate failure if requested
    if job.payload.get("fail"):
        raise Exception("simulated failure")
    
    logger.info(
        "Job processed successfully",
        extra={
            "job_id": job.id,
            "status": "done",
        }
    )


def _heartbeat_thread(worker_id: int) -> None:
    """
    Background thread that sends periodic worker heartbeats to Redis.
    
    Args:
        worker_id: Worker process ID
    """
    logger = get_logger(f"worker.{worker_id}.heartbeat")
    
    while True:
        try:
            time.sleep(5)
            r.setex(f"worker:{worker_id}:heartbeat", 10, "alive")
        except redis.RedisError as e:
            logger.error(
                f"Failed to update heartbeat: {e}",
                extra={"worker_id": worker_id, "status": "heartbeat_error"}
            )
        except Exception as e:
            logger.error(
                f"Heartbeat thread error: {e}",
                extra={"worker_id": worker_id}
            )


def _timeout_handler(signum: int, frame: object) -> None:
    """
    Signal handler for job timeout (SIGALRM).
    
    Args:
        signum: Signal number
        frame: Current stack frame
        
    Raises:
        TimeoutError: Always raised to interrupt job processing
    """
    raise TimeoutError()


def run_worker(worker_id: int) -> None:
    """
    Main worker loop that processes jobs from the queue.
    
    Continuously dequeues jobs, executes them with timeout protection,
    handles retries with exponential backoff, and moves failed jobs
    to the dead letter queue after max retries.
    
    Args:
        worker_id: Unique identifier for this worker process
    """
    logger = get_logger(f"worker.{worker_id}")
    
    # Start heartbeat thread
    heartbeat = threading.Thread(
        target=_heartbeat_thread,
        args=(worker_id,),
        daemon=True
    )
    heartbeat.start()
    
    logger.info(
        "Worker started",
        extra={"worker_id": worker_id, "status": "started"}
    )
    
    # Main worker loop
    while True:
        try:
            # Dequeue job with 5 second timeout
            job = dequeue(timeout=5)
            
            if job is None:
                continue
            
            # Set up job execution context
            job.worker_id = worker_id
            job.status = "running"
            job.started_at = time.time()
            update_status(job)
            
            logger.info(
                "Job started",
                extra={
                    "job_id": job.id,
                    "worker_id": worker_id,
                    "status": "running"
                }
            )
            
            start_time = time.time()
            
            # Execute job with timeout handling.
            # Prefer SIGALRM in main thread (works in production processes).
            # When running in a non-main thread (e.g., tests), fallback to
            # a thread-join timeout approach because signals aren't allowed.
            try:
                if threading.current_thread() is threading.main_thread():
                    signal.signal(signal.SIGALRM, _timeout_handler)
                    signal.alarm(job.timeout_seconds)
                    try:
                        process_job(job)
                        signal.alarm(0)

                        # Mark as done
                        job.status = "done"
                        job.completed_at = time.time()
                        duration_ms = int((job.completed_at - start_time) * 1000)
                        update_status(job)

                        logger.info(
                            "Job completed",
                            extra={
                                "job_id": job.id,
                                "worker_id": worker_id,
                                "status": "done",
                                "duration_ms": duration_ms
                            }
                        )
                    except Exception as e:
                        signal.alarm(0)
                        raise
                else:
                    proc_exc = None

                    def _runner():
                        nonlocal proc_exc
                        try:
                            process_job(job)
                        except Exception as ex:
                            proc_exc = ex

                    t = threading.Thread(target=_runner)
                    t.start()
                    t.join(job.timeout_seconds)

                    if t.is_alive():
                        # Timed out
                        raise TimeoutError()
                    if proc_exc:
                        raise proc_exc
                    # Success path
                    job.status = "done"
                    job.completed_at = time.time()
                    duration_ms = int((job.completed_at - start_time) * 1000)
                    update_status(job)

                    logger.info(
                        "Job completed",
                        extra={
                            "job_id": job.id,
                            "worker_id": worker_id,
                            "status": "done",
                            "duration_ms": duration_ms
                        }
                    )
            except Exception as e:
                # Increment retry counter
                job.retries += 1
                job.error = str(e)

                if job.retries < job.max_retries:
                    # Calculate exponential backoff delay
                    delay = 2 ** (job.retries - 1)

                    logger.info(
                        f"Job failed, retrying after {delay}s",
                        extra={
                            "job_id": job.id,
                            "worker_id": worker_id,
                            "status": "retry",
                            "attempt": job.retries,
                            "max_retries": job.max_retries,
                            "error": str(e)
                        }
                    )

                    # Sleep before retry
                    time.sleep(delay)

                    # Reset status and re-enqueue
                    job.status = "pending"
                    job.worker_id = None
                    update_status(job)
                    enqueue(job)
                else:
                    # Max retries exceeded
                    job.status = "failed"
                    update_status(job)
                    send_to_dlq(job)

                    logger.info(
                        "Job failed permanently",
                        extra={
                            "job_id": job.id,
                            "worker_id": worker_id,
                            "status": "failed",
                            "retries": job.retries,
                            "error": str(e)
                        }
                    )
        except redis.RedisError as e:
            logger.error(
                f"Redis error: {e}",
                extra={"worker_id": worker_id, "status": "redis_error"}
            )
            time.sleep(1)
        except KeyboardInterrupt:
            logger.info(
                "Worker shutting down",
                extra={"worker_id": worker_id, "status": "shutdown"}
            )
            break
        except Exception as e:
            logger.error(
                f"Unexpected worker error: {e}",
                extra={"worker_id": worker_id}
            )
            time.sleep(1)
