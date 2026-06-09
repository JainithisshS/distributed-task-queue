"""
FastAPI routes for task queue control and monitoring.

Provides endpoints for enqueueing jobs, checking status, viewing queue stats,
retrying failed jobs, and health checks.
"""

from datetime import datetime
from uuid import uuid4

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

from task_queue.broker import (
    enqueue,
    get_job,
    queue_stats,
    requeue_from_dlq,
)
from task_queue.job import Job

# Create FastAPI app
app = FastAPI(title="Distributed Task Queue Engine")


# Pydantic models for request/response
class EnqueueRequest(BaseModel):
    """Request model for job enqueueing."""
    
    payload: dict
    priority: str = "medium"
    timeout_seconds: int = 30


class EnqueueResponse(BaseModel):
    """Response model for job enqueueing."""
    
    job_id: str
    status: str
    priority: str


class JobResponse(BaseModel):
    """Response model for job details."""
    
    id: str
    payload: dict
    priority: str
    status: str
    retries: int
    max_retries: int
    timeout_seconds: int
    created_at: float
    started_at: float = None
    completed_at: float = None
    worker_id: int = None
    error: str = None


class QueueStatsResponse(BaseModel):
    """Response model for queue statistics."""
    
    queues: dict
    dlq: int


class RetryResponse(BaseModel):
    """Response model for retry operation."""
    
    message: str


class HealthResponse(BaseModel):
    """Response model for health check."""
    
    status: str
    timestamp: str


@app.post("/enqueue", response_model=EnqueueResponse)
async def enqueue_job(request: EnqueueRequest) -> EnqueueResponse:
    """
    Enqueue a new job to the task queue.
    
    Args:
        request: EnqueueRequest with payload, priority, and timeout
        
    Returns:
        EnqueueResponse with job_id, status, and priority
    """
    job = Job(
        id=str(uuid4()),
        payload=request.payload,
        priority=request.priority,
        timeout_seconds=request.timeout_seconds,
        status="pending"
    )
    
    enqueue(job)
    
    return EnqueueResponse(
        job_id=job.id,
        status=job.status,
        priority=job.priority
    )


@app.get("/jobs/{job_id}", response_model=JobResponse)
async def get_job_details(job_id: str) -> JobResponse:
    """
    Retrieve details for a specific job.
    
    Args:
        job_id: Unique job identifier
        
    Returns:
        JobResponse with full job details
        
    Raises:
        HTTPException: 404 if job not found
    """
    job = get_job(job_id)
    
    if job is None:
        raise HTTPException(status_code=404, detail="Job not found")
    
    return JobResponse(
        id=job.id,
        payload=job.payload,
        priority=job.priority,
        status=job.status,
        retries=job.retries,
        max_retries=job.max_retries,
        timeout_seconds=job.timeout_seconds,
        created_at=job.created_at,
        started_at=job.started_at,
        completed_at=job.completed_at,
        worker_id=job.worker_id,
        error=job.error
    )


@app.get("/queue/stats", response_model=QueueStatsResponse)
async def get_queue_stats() -> QueueStatsResponse:
    """
    Get current queue statistics.
    
    Returns:
        QueueStatsResponse with counts for each priority queue and DLQ
    """
    stats = queue_stats()
    
    return QueueStatsResponse(
        queues={
            "high": stats["high"],
            "medium": stats["medium"],
            "low": stats["low"]
        },
        dlq=stats["dlq"]
    )


@app.post("/jobs/{job_id}/retry", response_model=RetryResponse)
async def retry_job(job_id: str) -> RetryResponse:
    """
    Retry a failed job from the dead letter queue.
    
    Args:
        job_id: Unique job identifier
        
    Returns:
        RetryResponse with success message
        
    Raises:
        HTTPException: 404 if job not found in DLQ
    """
    success = requeue_from_dlq(job_id)
    
    if not success:
        raise HTTPException(
            status_code=404,
            detail="Job not found in DLQ"
        )
    
    return RetryResponse(message=f"Job {job_id} successfully requeued")


@app.get("/health", response_model=HealthResponse)
async def health_check() -> HealthResponse:
    """
    Health check endpoint.
    
    Returns:
        HealthResponse with status and timestamp
    """
    return HealthResponse(
        status="ok",
        timestamp=datetime.utcnow().isoformat()
    )
