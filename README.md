# queuectl_system
queuectl: A Minimal, Production-Grade CLI Job Queue
queuectl is a lightweight, persistent, and concurrent background job queue system built in Python. It uses SQLite for persistent storage, supports multiple parallel worker processes, and handles automatic retries with exponential backoff and a Dead Letter Queue (DLQ).

(A working CLI demo (video) would be linked here.)

Architecture Overview
queuectl is architected for robustness and simplicity, built on four main components:

Persistence Layer (persistence.py): Uses SQLite as a transactional, persistent backend. All database access is abstracted via a Repository pattern.
Core Logic (core.py): A State Machine that manages the job lifecycle (pending, processing, completed, failed, dead).
Execution Layer (worker.py): A multiprocessing-based worker system that fetches and executes jobs in parallel using subprocess.
Interface Layer (cli.py): A user-friendly CLI built with rich-click for all operations.
Job Lifecycle
Jobs transition through the following states:

pending: The initial state. The job is waiting for a worker.
processing: A worker has atomically dequeued the job and is executing it.
completed: The job's command exited with returncode 0.
failed: The job's command failed (non-zero exit) or timed out, but it still has retries left. It is rescheduled to pending with a run_at timestamp based on exponential backoff.
dead: The job failed and has exhausted all max_retries. It is now in the Dead Letter Queue (DLQ) and requires manual intervention (e.g., queuectl requeue).
Concurrency and Atomicity
The system is designed to be multi-process safe and prevent duplicate job execution. It achieves this using:

SQLite WAL Mode: The database is run in Write-Ahead Log (WAL) mode (PRAGMA journal_mode=WAL;), which allows for concurrent read/write access without "database is locked" errors.
Atomic Dequeue: Workers do not use a SELECT then UPDATE pattern. Instead, they use a single, atomic UPDATE... RETURNING * query to find, lock, and claim a job in one step, making race conditions impossible.
Setup Instructions
Clone the repository (or create the files):

Create the project structure as shown in the previous steps and copy/paste the code for each file.

Create and activate a virtual environment:

python -m venv.venv
source.venv/bin/activate
Install the package:

For development (editable mode):

pip install -e.
Initialize the database:

Before first use, you must initialize the SQLite database:

queuectl initdb
This creates the queue.db file in the current directory.

Usage Examples
queuectl provides a rich-click CLI for all operations.

1. Add a new job
$ queuectl add "echo 'Hello World'"
Job enqueued with ID: 1a7b...

# Add a job with max 5 retries
$ queuectl add "sleep 2" --max-retries 5

# Add a high-priority job
$ queuectl add "echo 'Urgent!'" --priority 10

# Add a job scheduled to run in the future
$ queuectl add "echo 'later'" --run-at "2025-12-01T10:00:00Z"
2. List jobs
# List pending jobs (the default)
$ queuectl list

# List completed jobs
$ queuectl list --state completed

# List the Dead Letter Queue (DLQ)
$ queuectl list --dlq
3. Run workers
# Start a single worker
$ queuectl worker

# Start 4 parallel worker processes
$ queuectl worker -n 4

# Start workers with a different backoff (3^attempts)
$ queuectl worker -n 4 --backoff-base 3
(Workers will print logs as they start, fetch, and complete jobs. Press Ctrl+C to initiate a graceful shutdown.)

4. Show job details (and output)
$ queuectl show <job-id>
5. Requeue a job from the DLQ
# Move a job from 'dead' back to 'pending'
$ queuectl requeue <job-id-from-dlq>
Assumptions & Trade-offs
Single-Host System: queuectl is designed as a single-host job queue. The SQLite database is a local file. It is not intended to be accessed over a network filesystem (NFS), which can lead to data corruption.
shell=True Security: The add command accepts a command string, which is executed with shell=True. This is a security risk (command injection). A more secure design would require commands as a JSON list (e.g., ["echo", "hello"]) and execute with shell=False.
WAL Write Serialization: While WAL mode allows concurrent reads, it serializes all write transactions. This architecture is robust, but it is not designed to compete with network brokers like Redis or RabbitMQ on raw, high-frequency write throughput.
Testing Instructions
This project uses pytest.

Install testing dependencies:

pip install pytest pytest-mock
Run the full test suite:

pytest
