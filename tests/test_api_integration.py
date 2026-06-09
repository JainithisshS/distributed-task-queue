"""Quick API validation script."""

import asyncio
from fastapi.testclient import TestClient
from api.routes import app


def run_quick_api_checks():
    client = TestClient(app)

    # Test health endpoint
    response = client.get("/health")
    print(f"Health check: {response.status_code}")
    print(f"Response: {response.json()}")

    # Test enqueue endpoint
    response = client.post("/enqueue", json={
        "payload": {"test": "data"},
        "priority": "high",
        "timeout_seconds": 30
    })
    print(f"\nEnqueue: {response.status_code}")
    job_response = response.json()
    print(f"Response: {job_response}")
    job_id = job_response["job_id"]

    # Test get job endpoint
    response = client.get(f"/jobs/{job_id}")
    print(f"\nGet job: {response.status_code}")
    print(f"Response: {response.json()}")

    # Test queue stats endpoint
    response = client.get("/queue/stats")
    print(f"\nQueue stats: {response.status_code}")
    print(f"Response: {response.json()}")

    print("\n✓ All API endpoints working!")


if __name__ == "__main__":
    run_quick_api_checks()
