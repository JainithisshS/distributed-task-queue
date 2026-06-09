from task_queue.job import Job
from task_queue.broker import enqueue

def main():
    job = Job(payload={"demo": True}, priority="high", timeout_seconds=10)
    enqueue(job)
    print(f"Enqueued job {job.id}")

if __name__ == '__main__':
    main()
