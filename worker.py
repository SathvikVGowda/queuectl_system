import multiprocessing
import signal
import time
import subprocess
from. import core
from.persistence import SQLiteJobRepository
from.core import WorkerConfig

class Worker(multiprocessing.Process):
    """
    A worker process that inherits from multiprocessing.Process.
    This provides full control over its lifecycle and signal handling.
    [88, 89, 90, 91, 92, 93, 94, 95, 96, 97]
    """

    def __init__(self, config: WorkerConfig, poll_interval=1):
        super().__init__()
        self.config = config
        self.poll_interval = poll_interval
        self.repository = None  # To be initialized in the new process
        self.shutdown_flag = multiprocessing.Event()
        self.current_job_id = None

    def run(self):
        """The main loop of the worker process."""
        
        # 1. Initialize DB connection *in this process*.
        #    (SQLite connections cannot be shared across processes)
        self.repository = SQLiteJobRepository()

        # 2. Install signal handlers for graceful shutdown [98, 99, 100, 101, 102]
        #    This worker process will now ignore SIGINT and handle
        #    it via the shutdown_flag.
        signal.signal(signal.SIGINT, self._handle_shutdown)
        signal.signal(signal.SIGTERM, self._handle_shutdown)

        print(f"Worker {self.pid} starting...")
        
        while not self.shutdown_flag.is_set():
            job = None
            try:
                job = self.repository.dequeue()
                if job:
                    self.current_job_id = job['id']
                    print(f"Worker {self.pid}: Processing job {job['id']}")
                    self.process_job(job)
                    self.current_job_id = None
                else:
                    # No jobs found, sleep to prevent busy-looping [35]
                    time.sleep(self.poll_interval)
            
            except Exception as e:
                # Top-level exception handler
                print(f"Worker {self.pid}: Unhandled exception: {e}")
                if job:
                    # If we crashed while processing, mark it as failed.
                    core.fail_job(self.repository, job, str(e), self.config)
        
        print(f"Worker {self.pid} shutting down gracefully.")

    def _handle_shutdown(self, sig, frame):
        """Signal handler to initiate graceful shutdown."""
        print(f"Worker {self.pid}: Shutdown signal received...")
        self.shutdown_flag.set()
        # If we are busy with a job, the main loop will exit
        # *after* the job is done.

    def process_job(self, job):
        """Executes the job command in a subprocess."""
        
        # (Bonus Feature) Job timeout
        JOB_TIMEOUT_SECONDS = 60  # This should be configurable
        
        try:
            # subprocess.run is the modern, blocking call [103, 104, 105, 106]
            result = subprocess.run(
                job['command'],
                shell=True,          # As required by user spec (e.g., "echo 'Hello'")
                capture_output=True, # Captures stdout/stderr [107, 108, 109, 110, 111, 112]
                text=True,           # Decodes stdout/stderr as strings
                timeout=JOB_TIMEOUT_SECONDS # (Bonus) [103, 113, 114, 115, 116]
            )
            
            # Job finished successfully
            if result.returncode == 0:
                core.complete_job(
                    self.repository, 
                    job, 
                    result.stdout, 
                    result.stderr
                )
            # Job failed with a non-zero exit code
            else:
                core.fail_job(
                    self.repository, 
                    job, 
                    result.stderr or "Job failed with non-zero exit code", 
                    self.config
                )
        
        # (Bonus) Job exceeded its timeout [113, 114]
        except subprocess.TimeoutExpired as e:
            error_message = f"Job timed out after {JOB_TIMEOUT_SECONDS}s."
            core.fail_job(self.repository, job, error_message, self.config)
        
        # Catch other exceptions (e.g., command not found)
        except Exception as e:
            error_message = f"Subprocess execution failed: {str(e)}"
            core.fail_job(self.repository, job, error_message, self.config)
