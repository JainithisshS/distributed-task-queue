"""
Main entry point for the distributed task queue engine.

Orchestrates startup of worker pool, watchdog, and FastAPI server.
Uses multiprocessing to run workers in separate processes.
"""

import multiprocessing
from multiprocessing import Process
from typing import List

import uvicorn

from task_queue.worker import run_worker
from task_queue.watchdog import run_watchdog
from api.routes import app

# Number of worker processes to spawn
NUM_WORKERS = 4


def start_workers() -> None:
    """
    Start a pool of worker processes.
    
    Creates a multiprocessing pool and maps run_worker across worker IDs.
    Blocks indefinitely while workers are running.
    """
    with multiprocessing.Pool(processes=NUM_WORKERS) as pool:
        # Map run_worker across all worker IDs (0 to NUM_WORKERS-1)
        pool.map(run_worker, range(NUM_WORKERS))
        
        # Keep pool alive
        pool.close()
        pool.join()


if __name__ == "__main__":
    # Create process for worker pool
    workers_process = Process(target=start_workers, daemon=False)
    workers_process.start()
    
    # Create process for watchdog
    watchdog_process = Process(target=run_watchdog, daemon=False)
    watchdog_process.start()
    
    # Run FastAPI server in main process
    uvicorn.run(app, host="0.0.0.0", port=8000)
    
    # Join processes (though they should run indefinitely)
    workers_process.join()
    watchdog_process.join()
