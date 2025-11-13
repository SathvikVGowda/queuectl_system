import sqlite3
import os

# Use an environment variable for the DB path, default to 'queue.db'
DB_PATH = os.environ.get("QUEUECTL_DB_PATH", "queue.db")

def get_connection():
    """
    Establishes a connection to the SQLite database.
    
    This connection is configured for robust multi-process concurrency
    by enabling Write-Ahead Log (WAL) mode.[13, 14, 15, 16, 17, 18, 19, 20, 21, 22]
    """
    conn = sqlite3.connect(DB_PATH, timeout=10)  # 10-second timeout for lock contention
    conn.row_factory = sqlite3.Row  # Access columns by name
    
    # --- CRITICAL CONCURRENCY CONFIGURATION ---
    
    # 1. Enable Write-Ahead Log (WAL) mode
    #    Allows concurrent reads and writes. [15, 16, 23, 18, 19]
    conn.execute("PRAGMA journal_mode = WAL;")
    
    # 2. Set Synchronous mode
    #    'NORMAL' is a safe and fast setting for WAL mode.
    conn.execute("PRAGMA synchronous = NORMAL;")
    
    return conn

def init_db():
    """Initializes the database schema."""
    schema = """
    CREATE TABLE IF NOT EXISTS jobs (
        id           TEXT PRIMARY KEY,
        command      TEXT NOT NULL,
        state        TEXT NOT NULL CHECK(state IN ('pending', 'processing', 'completed', 'failed', 'dead')),
        attempts     INTEGER NOT NULL DEFAULT 0,
        max_retries  INTEGER NOT NULL DEFAULT 3,
        created_at   TEXT NOT NULL,
        updated_at   TEXT NOT NULL,
        
        -- Bonus Feature Columns
        priority     INTEGER NOT NULL DEFAULT 0,
        run_at       TEXT DEFAULT NULL,
        stdout       TEXT DEFAULT NULL,
        stderr       TEXT DEFAULT NULL
    );

    CREATE INDEX IF NOT EXISTS idx_jobs_state_priority_created_at
    ON jobs (state, priority, created_at);
    
    CREATE INDEX IF NOT EXISTS idx_jobs_state_run_at
    ON jobs (state, run_at);
    """
    with get_connection() as conn:
        conn.executescript(schema)
        conn.commit()
