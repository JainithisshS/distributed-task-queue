"""
Structured JSON logging module for task queue system.

Provides a custom JsonFormatter that outputs JSON logs with contextual
information like worker_id, job_id, and duration.
"""

import json
import logging
from datetime import datetime
from typing import Optional


class JsonFormatter(logging.Formatter):
    """
    Custom formatter that outputs structured JSON logs.
    
    Includes contextual fields: timestamp, level, message, worker_id,
    job_id, status, and duration_ms.
    """

    def format(self, record: logging.LogRecord) -> str:
        """
        Format a log record as a JSON string.
        
        Args:
            record: LogRecord to format
            
        Returns:
            JSON-formatted log line
        """
        log_dict = {
            "timestamp": datetime.utcnow().isoformat(),
            "level": record.levelname,
            "message": record.getMessage(),
            "worker_id": getattr(record, "worker_id", None),
            "job_id": getattr(record, "job_id", None),
            "status": getattr(record, "status", None),
            "duration_ms": getattr(record, "duration_ms", None),
        }
        return json.dumps(log_dict)


def get_logger(name: str) -> logging.Logger:
    """
    Get a logger configured with JSON formatting.
    
    Args:
        name: Logger name (typically module name)
        
    Returns:
        Configured logger instance
    """
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)
    
    # Remove existing handlers to avoid duplicates
    logger.handlers.clear()
    
    handler = logging.StreamHandler()
    handler.setFormatter(JsonFormatter())
    logger.addHandler(handler)
    
    return logger
