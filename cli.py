import rich_click as click  # Drop-in replacement for 'click'
from rich.console import Console
from rich.table import Table
from rich.panel import Panel
import signal

from.db import init_db as _init_db
from.persistence import SQLiteJobRepository
from.core import WorkerConfig
from.worker import Worker

# --- Rich-Click Configuration ---
click.rich_click.STYLE_OPTION = "bold cyan"
click.rich_click.STYLE_ARGUMENT = "bold"
click.rich_click.STYLE_COMMAND = "bold"
click.rich_click.STYLE_METAVAR = "italic"
# --- End Configuration ---

@click.group()
def main():
    """
    queuectl: A minimal, production-grade CLI job queue system.
    
    Manages background jobs with worker processes, handles retries
    with exponential backoff, and maintains a Dead Letter Queue (DLQ).
    """
    pass

@main.command()
def initdb():
    """Initializes the job queue database."""
    _init_db()
    console = Console()
    console.print("[bold green]Database initialized successfully.[/bold green]")

@main.command()
@click.argument("command", type=str)
@click.option("--max-retries", type=int, default=3, help="Max retries before moving to DLQ.", show_default=True)
@click.option("--priority", type=int, default=0, help="Job priority (higher is first).", show_default=True)
@click.option("--run-at", type=str, help="Schedule job (ISO 8601 format: YYYY-MM-DDTHH:MM:SSZ).")
def add(command, max_retries, priority, run_at):
    """Enqueues a new job to be processed."""
    repo = SQLiteJobRepository()
    job_id = repo.add(command, max_retries, priority, run_at)
    console = Console()
    console.print(f"[bold green]Job enqueued with ID:[/bold green] {job_id}")

@main.command()
@click.option("--state", type=click.Choice(['pending', 'processing', 'completed', 'failed', 'dead']),
              default='pending', help="Filter by job state.", show_default=True)
@click.option("--dlq", is_flag=True, help="Alias for --state dead.")
@click.option("--limit", type=int, default=20, help="Number of jobs to show.", show_default=True)
def list(state, dlq, limit):
    """Lists jobs in the queue, color-coded by state."""
    repo = SQLiteJobRepository()
    if dlq:
        state = 'dead'
    
    jobs = repo.list_jobs(state=state, limit=limit)
    _print_job_table(jobs, f"{state.title()} Jobs")

@main.command()
@click.option("-n", "--workers", type=int, default=1, help="Number of worker processes to start.", show_default=True)
@click.option("--backoff-base", type=int, default=2, help="Base for exponential backoff (base ^ attempts).", show_default=True)
def worker(workers, backoff_base):
    """Starts one or more worker processes to process jobs."""
    if workers <= 0:
        click.echo("Number of workers must be at least 1.", err=True)
        return

    console = Console()
    console.print(f"[bold]Starting {workers} worker process(es)...[/bold]")
    console.print("Press [cyan]Ctrl+C[/cyan] to initiate graceful shutdown.")
    
    worker_config = WorkerConfig(backoff_base=backoff_base)
    worker_processes =
    for _ in range(workers):
        w = Worker(config=worker_config)
        w.start()
        worker_processes.append(w)

    def shutdown_all_workers(sig, frame):
        console.print("\n[bold red]Shutdown signal received.[/bold red] Terminating workers...")
        for w in worker_processes:
            w.terminate() # Sends SIGTERM
        for w in worker_processes:
            w.join()
        console.print("[bold green]All workers stopped.[/bold green]")

    # The main process handles SIGINT and delegates termination
    signal.signal(signal.SIGINT, shutdown_all_workers)
    signal.signal(signal.SIGTERM, shutdown_all_workers)

    # Wait for all worker processes to exit
    for w in worker_processes:
        w.join()

@main.command()
@click.argument("job_id", type=str)
def show(job_id):
    """Shows detailed info for a single job, including stdout/stderr."""
    console = Console()
    repo = SQLiteJobRepository()
    job = repo.get(job_id)
    
    if not job:
        console.print(f"[bold red]Error:[/bold red] Job {job_id} not found.")
        return

    # Helper to format state with color
    state = job['state']
    if state == 'completed': state = f"[green]{state}[/green]"
    if state == 'processing': state = f"[yellow]{state}[/yellow]"
    if state == 'failed': state = f"[orange]{state}[/orange]"
    if state == 'dead': state = f"[bold red]{state}[/bold red]"
    if state == 'pending': state = f"[dim]{state}[/dim]"

    content = f"""
[bold]ID[/bold]:         {job['id']}
[bold]State[/bold]:      {state}
[bold]Command[/bold]:    [cyan]{job['command']}[/cyan]
[bold]Attempts[/bold]:   {job['attempts']} / {job['max_retries']}
[bold]Priority[/bold]:   {job['priority']}
[bold]Created[/bold]:    {job['created_at']}
[bold]Updated[/bold]:    {job['updated_at']}
[bold]Run At[/bold]:     {job['run_at'] or 'N/A'}
    """
    console.print(Panel(content, title="Job Details", border_style="blue"))
    
    if job['stdout']:
        console.print(Panel(job['stdout'], title="stdout", border_style="green"))
    if job['stderr']:
        console.print(Panel(job['stderr'], title="stderr", border_style="red"))

@main.command()
@click.argument("job_id", type=str)
def requeue(job_id):
    """Moves a 'dead' job from the DLQ back to 'pending'."""
    console = Console()
    repo = SQLiteJobRepository()
    success = repo.requeue(job_id)
    if success:
        console.print(f"[bold green]Job {job_id} requeued to 'pending'.[/bold green]")
    else:
        console.print(f"[bold red]Error:[/bold red] Job {job_id} not found or not in 'dead' state.")

# --- Helper Functions for Rich Output ---

def _print_job_table(jobs, title):
    """Renders a list of jobs in a Rich table. [124, 125, 126, 127, 12, 128, 129, 130]"""
    console = Console()
    table = Table(title=title, border_style="blue")
    
    table.add_column("Job ID", style="cyan", no_wrap=True)
    table.add_column("State", style="white")
    table.add_column("Command", style="magenta")
    table.add_column("Attempts", justify="right", style="green")
    table.add_column("Created At", style="default")
    table.add_column("Run At", style="yellow")

    def format_state(state):
        if state == 'completed': return "[green]completed[/green]"
        if state == 'processing': return "[yellow]processing[/yellow]"
        if state == 'failed': return "[orange]failed[/orange]"
        if state == 'dead': return "[bold red]dead[/bold red]"
        return "[dim]pending[/dim]"

    for job in jobs:
        table.add_row(
            job['id'],
            format_state(job['state']),
            job['command'],
            f"{job['attempts']} / {job['max_retries']}",
            job['created_at'],
            job['run_at'] or "ASAP"
        )
    
    console.print(table)
