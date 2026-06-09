"""
Benchmark script for the distributed task queue engine.

Modes:
  python tools/benchmark.py           -- broker microbenchmarks (no Redis needed)
  python tools/benchmark.py --real    -- end-to-end benchmark (requires Docker)

Broker mode:
  Uses fakeredis (in-process). Measures pure broker overhead:
  serialisation, Redis list operations, priority routing.

Real mode:
  Connects to real Redis (localhost:6379). Spawns actual worker processes.
  Measures full job lifecycle: enqueue -> broker -> worker -> done status.
  Requires:  docker compose up -d
"""

import argparse
import multiprocessing
import statistics
import sys
import threading
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

import fakeredis
import redis as redis_lib
import task_queue.broker as broker_module
from task_queue.broker import dequeue, enqueue, get_job, queue_stats
from task_queue.job import Job
from task_queue.worker import run_worker


# ── Helpers ───────────────────────────────────────────────────────────────────

def fresh_fake_redis():
    r = fakeredis.FakeRedis(decode_responses=True)
    broker_module.r = r
    return r


def connect_real_redis():
    r = redis_lib.Redis(host="localhost", port=6379, decode_responses=True)
    r.ping()  # raises if not reachable
    broker_module.r = r
    r.flushdb()  # clean slate
    return r


def header(title: str):
    print(f"\n{'=' * 60}")
    print(f"  {title}")
    print(f"{'=' * 60}")


def result(label: str, value: str):
    print(f"  {label:<42} {value}")


# ── Microbenchmarks ───────────────────────────────────────────────────────────

def bench_enqueue(n: int = 5_000):
    header(f"Enqueue throughput  ({n:,} jobs, single-threaded)")
    fresh_fake_redis()
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


def bench_dequeue(n: int = 5_000):
    header(f"Dequeue throughput  ({n:,} jobs, single-threaded)")
    fresh_fake_redis()
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


def bench_latency(n: int = 1_000):
    header(f"Round-trip broker latency  ({n:,} jobs)")
    fresh_fake_redis()
    latencies_ms = []
    for i in range(n):
        job = Job(priority="high", payload={"i": i})
        t0 = time.perf_counter()
        enqueue(job)
        r = dequeue(timeout=1)
        t1 = time.perf_counter()
        if r:
            latencies_ms.append((t1 - t0) * 1000)
    p50 = statistics.median(latencies_ms)
    p95 = sorted(latencies_ms)[int(len(latencies_ms) * 0.95)]
    p99 = sorted(latencies_ms)[int(len(latencies_ms) * 0.99)]
    mean = statistics.mean(latencies_ms)
    result("Jobs measured:", f"{len(latencies_ms):,}")
    result("Mean latency:", f"{mean:.2f} ms")
    result("p50 latency:", f"{p50:.2f} ms")
    result("p95 latency:", f"{p95:.2f} ms")
    result("p99 latency:", f"{p99:.2f} ms")
    return {"mean": mean, "p50": p50, "p95": p95, "p99": p99}


def bench_priority(n_per_level: int = 500):
    header(f"Priority ordering  ({n_per_level * 3:,} mixed jobs)")
    fresh_fake_redis()
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
    first_low = next((i for i, p in enumerate(order) if p == "low"), len(order))
    highs_before_low = sum(1 for p in order[:first_low] if p == "high")
    result("Total jobs processed:", f"{len(order):,}")
    result("High-priority before first low:", f"{highs_before_low:,} / {n_per_level:,}")
    result("Priority ordering correct:", "YES" if highs_before_low == n_per_level else "PARTIAL")
    return highs_before_low == n_per_level


def bench_concurrent_producers(n: int = 10_000, threads: int = 4):
    header(f"Concurrent enqueue  ({n:,} jobs, {threads} threads)")
    fresh_fake_redis()
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


# ── End-to-end benchmark (real Redis + real workers) ─────────────────────────

def bench_end_to_end(n: int = 500, num_workers: int = 4):
    """
    Full lifecycle benchmark: enqueue -> real Redis -> multiprocessing workers -> done.
    Jobs use noop=True so process_job returns immediately — measures system
    overhead only (IPC, Redis round-trips, status updates), not task execution time.
    """
    header(f"End-to-end throughput  ({n:,} noop jobs, {num_workers} workers)")

    try:
        connect_real_redis()
    except Exception:
        print("  ERROR: Could not connect to Redis on localhost:6379")
        print("  Run:  docker run -d -p 6379:6379 redis:7  then retry.")
        return None

    # Spawn workers first, let them boot and connect to Redis
    procs = []
    for wid in range(num_workers):
        p = multiprocessing.Process(target=run_worker, args=(wid,), daemon=True)
        p.start()
        procs.append(p)

    print(f"  Waiting 3s for {num_workers} workers to connect to Redis...")
    time.sleep(3)

    # Enqueue all jobs — start timer HERE, after workers are warm
    job_ids = []
    t_enqueue_start = time.perf_counter()
    for i in range(n):
        job = Job(priority="high", payload={"noop": True, "i": i})
        enqueue(job)
        job_ids.append(job.id)
    t_enqueue_done = time.perf_counter()

    result("Jobs enqueued:", f"{n:,}  ({(t_enqueue_done - t_enqueue_start):.2f}s)")
    result("Workers:", str(num_workers))
    print("  Polling for completion", end="", flush=True)

    # Poll until all jobs reach "done" status
    t_start = time.perf_counter()
    deadline = t_start + 120  # 2-minute hard timeout

    last_done = 0
    while True:
        done_count = sum(
            1 for jid in job_ids
            if (j := get_job(jid)) and j.status == "done"
        )
        if done_count != last_done:
            print(f"\r  Polling: {done_count}/{n} done... ", end="", flush=True)
            last_done = done_count
        if done_count >= n:
            break
        if time.perf_counter() > deadline:
            print(f"\r  Timed out after 120s. {done_count}/{n} completed.")
            break
        time.sleep(0.05)

    elapsed = time.perf_counter() - t_start
    print()  # newline after progress

    # Kill workers
    for p in procs:
        p.terminate()
        p.join(timeout=2)

    done_final = sum(
        1 for jid in job_ids
        if (j := get_job(jid)) and j.status == "done"
    )
    rate = done_final / elapsed if elapsed > 0 else 0

    result("Jobs completed:", f"{done_final:,} / {n:,}")
    result("Elapsed (post-enqueue):", f"{elapsed:.2f} s")
    result("End-to-end throughput:", f"{rate:,.1f} jobs/sec  ({num_workers} workers)")
    result("Per-worker throughput:", f"{rate / num_workers:,.1f} jobs/sec/worker")
    return {"total": rate, "per_worker": rate / num_workers, "done": done_final}



# ── Summary ───────────────────────────────────────────────────────────────────

def summary_micro(enq_rate, deq_rate, latency, conc_rate):
    header("BROKER MICROBENCHMARK SUMMARY  (fakeredis, no network)")
    result("Single-threaded enqueue:", f"{enq_rate:,.0f} jobs/sec")
    result("Single-threaded dequeue:", f"{deq_rate:,.0f} jobs/sec")
    result("4-thread concurrent enqueue:", f"{conc_rate:,.0f} jobs/sec")
    result("Round-trip latency p50:", f"{latency['p50']:.2f} ms")
    result("Round-trip latency p99:", f"{latency['p99']:.2f} ms")
    print()


def summary_real(e2e):
    header("END-TO-END SUMMARY  (real Redis + real workers)")
    if e2e:
        result("Total throughput (4 workers):", f"{e2e['total']:,.1f} jobs/sec")
        result("Per-worker throughput:", f"{e2e['per_worker']:,.1f} jobs/sec/worker")
        result("Jobs completed:", f"{e2e['done']:,}")
    print()


# ── Entry point ───────────────────────────────────────────────────────────────

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Distributed Task Queue Benchmark")
    parser.add_argument("--real", action="store_true",
                        help="Run end-to-end benchmark (requires: docker compose up -d)")
    parser.add_argument("--jobs", type=int, default=500,
                        help="Number of jobs for --real benchmark (default: 500)")
    parser.add_argument("--workers", type=int, default=4,
                        help="Number of worker processes for --real benchmark (default: 4)")
    args = parser.parse_args()

    if args.real:
        print("\n  Distributed Task Queue -- End-to-End Benchmark")
        print("  Requires: docker compose up -d\n")
        e2e = bench_end_to_end(n=args.jobs, num_workers=args.workers)
        summary_real(e2e)
    else:
        print("\n  Distributed Task Queue -- Broker Microbenchmarks")
        print("  Uses fakeredis (in-process). No Redis/Docker required.\n")
        enq  = bench_enqueue(5_000)
        deq  = bench_dequeue(5_000)
        lat  = bench_latency(1_000)
        bench_priority(500)
        conc = bench_concurrent_producers(10_000, threads=4)
        summary_micro(enq, deq, lat, conc)
