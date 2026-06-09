"""
Job schema module for task queue system.

Defines the Job dataclass with serialization/deserialization methods
for persisting job state to Redis.
"""

import dataclasses
import json
import time
import uuid
from typing import Any, Dict, Optional


@dataclasses.dataclass
class Job:
    """
    Represents a task in the distributed queue.
    
    Attributes:
        id: Unique job identifier (auto-generated UUID if not provided)
        payload: Dict containing task data
        priority: Job priority level (high, medium, low)
        status: Current job status (pending, running, done, failed)
        retries: Number of retry attempts so far
        max_retries: Maximum number of retry attempts allowed
        timeout_seconds: Job execution timeout in seconds
        created_at: Timestamp when job was created
        started_at: Timestamp when job execution started
        completed_at: Timestamp when job execution completed
        worker_id: ID of worker processing this job
        error: Error message if job failed
    """
    
    id: str = dataclasses.field(default_factory=lambda: str(uuid.uuid4()))
    payload: Dict[str, Any] = dataclasses.field(default_factory=dict)
    priority: str = "medium"
    status: str = "pending"
    retries: int = 0
    max_retries: int = 3
    timeout_seconds: int = 30
    created_at: float = dataclasses.field(default_factory=time.time)
    started_at: Optional[float] = None
    completed_at: Optional[float] = None
    worker_id: Optional[int] = None
    error: Optional[str] = None

    def to_json(self) -> str:
        """
        Serialize the job to a JSON string.
        
        Returns:
            JSON string representation of the job
        """
        return json.dumps(dataclasses.asdict(self))

    @classmethod
    def from_json(cls, data: str) -> "Job":
        """
        Deserialize a job from a JSON string.
        
        Args:
            data: JSON string representation of a job
            
        Returns:
            Reconstructed Job object
        """
        parsed = json.loads(data)
        return cls(**parsed)
