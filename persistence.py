import sqlite3
import uuid
import datetime
from.db import get_connection

def get_iso_now():
    """Returns the current time in ISO 8601 UTC format (with 'Z')."""
    # [29, 30, 31, 32, 33]
    return datetime.datetime.now(datetime.timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')

class SQLiteJobRepository:
    """
    Manages all data access operations for jobs, ensuring that
    all database logic is centralized in this one file.
    """

    def add(self, command, max_retries, priority, run_at):
        """Adds a new job to the queue in a 'pending' state."""
        job_id = str(uuid.uuid4())
        now = get_iso_now()
        
        sql = """
        INSERT INTO jobs (id, command, state, max_retries, priority, run_at, created_at, updated_at)
        VALUES (?,?, 'pending',?,?,?,?,?)
        """
        
        with get_connection() as conn:
            conn.execute(sql, [job_id, command, max_retries, priority, run_at, now, now])
            conn.commit()
        return job_id

    def get(self, job_id):
        """Fetches a single job by its ID."""
        with get_connection() as conn:
            cursor = conn.execute("SELECT * FROM jobs WHERE id =?", [job_id])
            row = cursor.fetchone()
            return dict(row) if row else None

    def list_jobs(self, state, limit):
        """Lists all jobs in a given state, ordered by creation time."""
        sql = "SELECT * FROM jobs WHERE state =? ORDER BY created_at ASC LIMIT?"
        with get_connection() as conn:
            cursor = conn.execute(sql, [state, limit])
            return [dict(row) for row in cursor.fetchall()]

    def update_state(self, job_id, state, stdout=None, stderr=None, run_at=None):
        """Updates the state and output of a job."""
        now = get_iso_now()
        sql = """
        UPDATE jobs
        SET state =?,
            updated_at =?,
            stdout = COALESCE(?, stdout),
            stderr = COALESCE(?, stderr),
            run_at =?
        WHERE id =?
        """
        with get_connection() as conn:
            conn.execute(sql, [state, now, stdout, stderr, run_at, job_id])
            conn.commit()

    def requeue(self, job_id):
        """Moves a 'dead' job back to the 'pending' state."""
        now = get_iso_now()
        sql = """
        UPDATE jobs
        SET state = 'pending',
            attempts = 0,
            updated_at =?,
            run_at = NULL,
            stdout = NULL,
            stderr = NULL
        WHERE id =? AND state = 'dead'
        """
        with get_connection() as conn:
            cursor = conn.execute(sql, [now, job_id])
            conn.commit()
            return cursor.rowcount > 0  # True if update was successful

    def dequeue(self):
        """
        Atomically selects the highest-priority, ready-to-run job,
        updates its state to 'processing', and returns it.
        This prevents race conditions.[34, 35, 36, 37, 38, 39, 40, 41, 42, 43, 44, 45, 46, 47, 48, 49, 50, 51, 52, 53, 54, 55, 56, 57, 58, 59, 60]
        """
        
        # This "Golden Query" performs the find, update, and return
        # in a single, atomic operation, which is the key to
        # concurrency-safe queuing in SQLite.
        sql = f"""
        UPDATE jobs
        SET state = 'processing',
            updated_at = strftime('%Y-%m-%dT%H:%M:%SZ', 'now'),
            attempts = attempts + 1
        WHERE id = (
            SELECT id FROM jobs
            WHERE 
                state = 'pending'
                -- (Bonus) Handle scheduled jobs [61, 62, 54, 63, 64, 65, 66]
                AND (run_at IS NULL OR run_at <= strftime('%Y-%m-%dT%H:%M:%SZ', 'now'))
            ORDER BY
                priority DESC,  -- (Bonus) Highest priority first [66, 67, 68, 69]
                created_at ASC  -- FIFO within priority
            LIMIT 1             -- Get only one job [42, 43]
        )
        RETURNING *;  -- Return the locked job to the caller [40, 45, 47]
        """
        
        try:
            with get_connection() as conn:
                # BEGIN IMMEDIATE acquires a write lock immediately
                # to prevent deadlocks from lock-upgrade contention.[70]
                conn.execute('BEGIN IMMEDIATE TRANSACTION')
                try:
                    cursor = conn.execute(sql)
                    job_row = cursor.fetchone()
                    conn.commit()
                    
                    if job_row:
                        return dict(job_row)
                    return None  # Queue was empty
                except Exception:
                    conn.rollback()
                    raise
        except sqlite3.OperationalError as e:
            # This can happen under high contention ("database is locked")
            # The worker will simply retry the dequeue.
            print(f"Dequeue failed due to contention: {e}")
            return None
