import datetime
import random
import math
from.persistence import SQLiteJobRepository

class WorkerConfig:
    """A simple container for worker configuration."""
    def __init__(self, backoff_base=2):
        self.backoff_base = float(backoff_base)

def complete_job(repository: SQLiteJobRepository, job, stdout, stderr):
    """Marks a job as 'completed' and logs its output."""
    print(f"Job {job['id']} completed successfully.")
    repository.update_state(job['id'], 'completed', stdout=stdout, stderr=stderr)

def fail_job(repository: SQLiteJobRepository, job, stderr, config: WorkerConfig):
    """
    Handles a failed job.
    Increments attempt counter. If retries are exhausted, moves to DLQ ('dead').
    Otherwise, calculates backoff and reschedules it ('pending' with 'run_at').
    [35, 81, 37, 82]
    """
    if job['attempts'] >= job['max_retries']:
        # Retries exhausted, move to Dead Letter Queue (DLQ)
        print(f"Job {job['id']} failed. Max retries ({job['max_retries']}) reached. Moving to DLQ.")
        repository.update_state(job['id'], 'dead', stderr=stderr)
    else:
        # Job is retryable.
        # Calculate exponential backoff and reschedule.
        delay_seconds, next_run_at = calculate_backoff(job, config)
        
        print(f"Job {job['id']} failed. Retrying in {delay_seconds:.2f}s...")
        
        # Set state back to 'pending' but with a future 'run_at' time.
        repository.update_state(
            job['id'], 
            'pending', 
            stderr=stderr, 
            run_at=next_run_at
        )

def calculate_backoff(job, config: WorkerConfig):
    """
    Calculates the exponential backoff delay based on the user's formula.
    Returns (delay_seconds, next_run_at_iso_string).
    [83, 84, 85, 86, 87]
    """
    attempts = job['attempts']
    
    # Calculate delay: delay = base ^ attempts
    delay_seconds = config.backoff_base ** attempts
    
    # Add jitter (e.g., +/- 20%) to prevent thundering herd
    jitter = random.uniform(0.8, 1.2)
    delay_with_jitter = delay_seconds * jitter
    
    next_run_time = (
        datetime.datetime.now(datetime.timezone.utc) + 
        datetime.timedelta(seconds=delay_with_jitter)
    )
    
    # Format for SQLite [29, 30, 31, 33]
    next_run_at_iso = next_run_time.strftime('%Y-%m-%dT%H:%M:%SZ')
    
    return delay_with_jitter, next_run_at_iso
