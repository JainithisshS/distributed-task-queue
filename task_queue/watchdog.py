"""
Watchdog module for job recovery.

Monitors running jobs and detects worker failures by checking heartbeat keys.
Orphaned jobs are automatically re-enqueued for processing.
"""

import time
from typing import Optional

import redis

from task_queue.broker import (
    JOB_PREFIX,
    enqueue,
    update_status,
    r,
)
from task_queue.job import Job
from task_queue.logger import get_logger


def run_watchdog() -> None:
    """
    Monitor all running jobs and recover orphaned jobs.
    
    Runs continuously, checking every 15 seconds for jobs marked as running
    that no longer have an active worker heartbeat. Re-enqueues such jobs
    for processing.
    
    Note: In production, use SCAN instead of KEYS to avoid blocking Redis.
    KEYS blocks the entire Redis server and should not be used on live
    systems with significant data. SCAN is iterative and non-blocking.
    """
    logger = get_logger("watchdog")
    
    logger.info(
        "Watchdog started",
        extra={"status": "started"}
    )
    
    while True:
        try:
            time.sleep(15)
            
            # Get all job keys
            # NOTE: In production, use SCAN instead of KEYS
            job_keys = r.keys(f"{JOB_PREFIX}*")
            
            for job_key in job_keys:
                try:
                    # Fetch job
                    raw_job = r.hget(job_key, "data")
                    
                    if raw_job is None:
                        continue
                    
                    job = Job.from_json(raw_job)
                    
                    # Check if job is running
                    if job.status != "running":
                        continue
                    
                    # Check if worker heartbeat exists
                    heartbeat_key = f"worker:{job.worker_id}:heartbeat"
                    heartbeat_exists = r.exists(heartbeat_key)
                    
                    if not heartbeat_exists:
                        # Worker is dead, recover the job
                        logger.info(
                            "Worker dead, recovering orphaned job",
                            extra={
                                "job_id": job.id,
                                "worker_id": job.worker_id,
                                "status": "recovered"
                            }
                        )
                        
                        # Reset job state
                        job.status = "pending"
                        job.worker_id = None
                        update_status(job)
                        enqueue(job)
                        
                        logger.info(
                            "Job re-enqueued",
                            extra={
                                "job_id": job.id,
                                "status": "re-enqueued"
                            }
                        )
                except redis.RedisError as e:
                    logger.error(
                        f"Redis error checking job {job_key}: {e}",
                        extra={"status": "redis_error"}
                    )
                except Exception as e:
                    logger.error(
                        f"Error processing job {job_key}: {e}",
                        extra={"status": "process_error"}
                    )
        except redis.RedisError as e:
            logger.error(
                f"Redis error in watchdog: {e}",
                extra={"status": "redis_error"}
            )
            time.sleep(5)
        except KeyboardInterrupt:
            logger.info(
                "Watchdog shutting down",
                extra={"status": "shutdown"}
            )
            break
        except Exception as e:
            logger.error(
                f"Unexpected watchdog error: {e}",
                extra={"status": "error"}
            )
            time.sleep(5)
