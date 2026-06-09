"""
Benchmark script for the distributed task queue engine.

Measures:
  - Enqueue throughput (jobs/sec)
  - Dequeue throughput (jobs/sec)
  - End-to-end broker latency per job (ms)
  - Priority routing correctness under load
  - Concurrent worker throughput simulation

Run with:  python tools/benchmark.py
No Redis required — uses fakeredis for isolation.
"""

import statistics
import sys
import threading
import time
from pathlib import Path

# Allow imports from project root
sys.path.insert(0, str(Path(__file__).parent.parent))

import fakeredis
import task_queue.broker as broker_module
from task_queue.broker import (
    dequeue,
    enqueue,
    queue_stats,
)
from task_queue.job import Job

# ── Helpers ───────────────────────────────────────────────────────────────────

def fresh_redis():
    """Swap in a clean fakeredis instance."""
    r = fakeredis.FakeRedis(decode_responses=True)
    broker_module.r = r
    return r


def header(title: str):
    print(f"\n{'=' * 56}")
    print(f"  {title}")
    print(f"{'=' * 56}")


def result(label: str, value: str):
    print(f"  {label:<38} {value}")


# ── Benchmark 1: Enqueue throughput ──────────────────────────────────────────

def bench_enqueue(n: int = 5_000):
    header(f"Enqueue throughput  ({n:,} jobs)")
    fresh_redis()

    jobs = [Job(priority="high", payload={"i": i}) for i in range(n)]

    start = time.perf_counter()
    for job in jobs:
        enqueue(job)
    elapsed = time.perf_counter() - start

    rate = n / elapsed
    result("Jobs enqueued:", f"{n:,}")
    result("Elapsed:", f"{elapsed:.3f} s")
    result("Throughput:", f"{rate:,.0f} jobs/sec")
    return rate


# ── Benchmark 2: Dequeue throughput ──────────────────────────────────────────

def bench_dequeue(n: int = 5_000):
    header(f"Dequeue throughput  ({n:,} jobs)")
    r = fresh_redis()

    jobs = [Job(priority="medium", payload={"i": i}) for i in range(n)]
    for job in jobs:
        enqueue(job)

    start = time.perf_counter()
    dequeued = 0
    while dequeued < n:
        job = dequeue(timeout=1)
        if job:
            dequeued += 1
    elapsed = time.perf_counter() - start

    rate = n / elapsed
    result("Jobs dequeued:", f"{n:,}")
    result("Elapsed:", f"{elapsed:.3f} s")
    result("Throughput:", f"{rate:,.0f} jobs/sec")
    return rate


# ── Benchmark 3: End-to-end broker latency ────────────────────────────────────

def bench_latency(n: int = 1_000):
    header(f"Round-trip broker latency  ({n:,} jobs)")
    fresh_redis()

    latencies_ms = []
    for i in range(n):
        job = Job(priority="high", payload={"i": i})
        t0 = time.perf_counter()
        enqueue(job)
        result_job = dequeue(timeout=1)
        t1 = time.perf_counter()
        if result_job:
            latencies_ms.append((t1 - t0) * 1000)

    p50  = statistics.median(latencies_ms)
    p95  = sorted(latencies_ms)[int(len(latencies_ms) * 0.95)]
    p99  = sorted(latencies_ms)[int(len(latencies_ms) * 0.99)]
    mean = statistics.mean(latencies_ms)

    result("Jobs measured:", f"{len(latencies_ms):,}")
    result("Mean latency:", f"{mean:.2f} ms")
    result("p50 latency:", f"{p50:.2f} ms")
    result("p95 latency:", f"{p95:.2f} ms")
    result("p99 latency:", f"{p99:.2f} ms")
    return {"mean": mean, "p50": p50, "p95": p95, "p99": p99}


# ── Benchmark 4: Priority ordering under load ────────────────────────────────

def bench_priority(n_per_level: int = 500):
    header(f"Priority ordering  ({n_per_level * 3:,} mixed jobs)")
    fresh_redis()

    # Interleave enqueue across priorities to simulate real load
    for i in range(n_per_level):
        enqueue(Job(priority="low",    payload={"i": i}))
        enqueue(Job(priority="medium", payload={"i": i}))
        enqueue(Job(priority="high",   payload={"i": i}))

    order = []
    total = n_per_level * 3
    while len(order) < total:
        job = dequeue(timeout=1)
        if job:
            order.append(job.priority)

    # Count how many high-priority jobs came before any low-priority job
    first_low = next((i for i, p in enumerate(order) if p == "low"), len(order))
    highs_before_low = sum(1 for p in order[:first_low] if p == "high")

    result("Total jobs processed:", f"{len(order):,}")
    result("High-priority jobs before first low:", f"{highs_before_low:,} / {n_per_level:,}")
    result("Priority ordering correct:", "YES" if highs_before_low == n_per_level else "PARTIAL")


# ── Benchmark 5: Concurrent producer throughput ───────────────────────────────

def bench_concurrent_producers(n: int = 10_000, threads: int = 4):
    header(f"Concurrent enqueue  ({n:,} jobs, {threads} threads)")
    fresh_redis()

    jobs_per_thread = n // threads
    errors = []

    def producer(tid: int):
        for i in range(jobs_per_thread):
            try:
                enqueue(Job(priority="medium", payload={"thread": tid, "i": i}))
            except Exception as e:
                errors.append(e)

    workers = [threading.Thread(target=producer, args=(t,)) for t in range(threads)]

    start = time.perf_counter()
    for w in workers:
        w.start()
    for w in workers:
        w.join()
    elapsed = time.perf_counter() - start

    actual = jobs_per_thread * threads
    rate = actual / elapsed
    result("Threads:", str(threads))
    result("Jobs enqueued:", f"{actual:,}")
    result("Elapsed:", f"{elapsed:.3f} s")
    result("Throughput:", f"{rate:,.0f} jobs/sec")
    result("Errors:", str(len(errors)))
    return rate


# ── Summary ───────────────────────────────────────────────────────────────────

def summary(enq_rate, deq_rate, latency, conc_rate):
    header("SUMMARY")
    result("Single-threaded enqueue:", f"{enq_rate:,.0f} jobs/sec")
    result("Single-threaded dequeue:", f"{deq_rate:,.0f} jobs/sec")
    result("4-thread concurrent enqueue:", f"{conc_rate:,.0f} jobs/sec")
    result("Round-trip latency (p50):", f"{latency['p50']:.2f} ms")
    result("Round-trip latency (p99):", f"{latency['p99']:.2f} ms")
    print()


if __name__ == "__main__":
    print("\n  Distributed Task Queue — Performance Benchmark")
    print("  All tests use fakeredis (in-process) for isolation\n")

    enq  = bench_enqueue(5_000)
    deq  = bench_dequeue(5_000)
    lat  = bench_latency(1_000)
    bench_priority(500)
    conc = bench_concurrent_producers(10_000, threads=4)
    summary(enq, deq, lat, conc)
