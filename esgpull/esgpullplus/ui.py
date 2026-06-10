# rich
from rich.progress import (
    Progress,
    BarColumn,
    TimeElapsedColumn,
    DownloadColumn,
    TransferSpeedColumn,
    TimeRemainingColumn,
)
import logging
from datetime import datetime
from pathlib import Path
from typing import Optional

# custom
from esgpull.esgpullplus import config

# TODO: remove FAILED files from UI after 10s (like with successful files)
# TODO: fix incorrectly marked failed files
# TODO: fix files mislabelled as cancelled in final summary

_DOWNLOAD_ERROR_LOGGER_NAME = "esgpull.download_errors"
_download_error_log_path: Optional[Path] = None


def init_download_error_log() -> Path:
    """Create one error log file per API run; reused across download batches."""
    global _download_error_log_path
    if _download_error_log_path is not None:
        return _download_error_log_path

    log_dir = config.log_dir
    log_dir.mkdir(exist_ok=True)
    timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    _download_error_log_path = log_dir / f"download_errors_{timestamp}.log"

    logger = logging.getLogger(_DOWNLOAD_ERROR_LOGGER_NAME)
    logger.setLevel(logging.ERROR)
    logger.propagate = False
    if not logger.handlers:
        handler = logging.FileHandler(_download_error_log_path)
        handler.setFormatter(
            logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
        )
        logger.addHandler(handler)

    return _download_error_log_path


def get_download_error_log_path() -> Optional[Path]:
    return _download_error_log_path


class DownloadProgressUI:
    """Manages rich progress bars for overall and per-file download, with status in the bar description."""

    def __init__(self, files):
        self.files = files
        self.progress = Progress(
            "[progress.description]{task.description}",
            BarColumn(),
            "[progress.percentage]{task.percentage:>3.0f}%",
            DownloadColumn(),
            TransferSpeedColumn(),
            TimeRemainingColumn(),
            TimeElapsedColumn(),
            transient=True,
            expand=True,
            auto_refresh=True,
            refresh_per_second=10,
        )
        self.overall_task = None
        self.status_counts = {
            "done": 0,
            "skipped": 0,
            "failed": 0,
            "unknown": 0,
        }
        self.failed_files = []  # List of (file, error_message)
        self.file_task_ids = {}  # file -> task_id
        self.file_status = {}  # file -> status string
        self._setup_logger()

    def _setup_logger(self):
        init_download_error_log()
        self.logger = logging.getLogger(_DOWNLOAD_ERROR_LOGGER_NAME)

    def __enter__(self):
        self.progress.__enter__()
        self.overall_task = self.progress.add_task(
            "[cyan]Overall\n", total=len(self.files)
        )
        # Add a progress bar for each file, with initial status 'PENDING'
        for file in self.files:
            fname = file.filename
            desc = f"[white][PENDING] {fname}"
            # Set total to file.size if available, else 1
            total = getattr(file, "size", None) or 1
            task_id = self.progress.add_task(desc, total=total, visible=True)
            self.file_task_ids[file.file_id] = task_id
            self.file_status[file.file_id] = "PENDING"
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.progress.__exit__(exc_type, exc_val, exc_tb)

    def set_status(self, file, status, color=None):
        """Update the status for a file and update the progress bar description."""
        task_id = self.file_task_ids.get(file.file_id)
        if task_id is None:
            return
        fname = file.filename
        color = color or "white"
        desc = f"[{color}][{status}] {fname}"
        self.progress.update(task_id, description=desc)
        self.file_status[fname] = status
        if status == "DONE":
            self.status_counts["done"] += 1
        elif status in ("SKIPPED", "SKIP"):
            self.status_counts["skipped"] += 1
        elif status in ("FAIL", "FAILED", "ERROR", "TIMEOUT"):
            self.status_counts["failed"] += 1
        # else:
        #     print(status)
        #     self.status_counts["unknown"] += 1

    def add_failed(self, file, msg, exc_info=False):
        self.failed_files.append((file, msg))
        init_download_error_log()
        self.logger.error(
            "%s | node=%s | %s",
            file.filename,
            getattr(file, "data_node", "Unknown"),
            msg,
            exc_info=exc_info,
        )
        for handler in self.logger.handlers:
            handler.flush()

    def complete_file(self, file):
        # Advance overall progress and mark file bar as complete
        task_id = self.file_task_ids.get(file.file_id)
        if task_id is None:
            return
        self.progress.update(task_id, completed=self.progress.tasks[task_id].total)
        if self.overall_task is not None:
            self.progress.advance(self.overall_task)

    def update_file_progress(self, file, completed, total=None):
        task_id = self.file_task_ids.get(file.file_id)
        if task_id is None:
            return
        if total is not None:
            self.progress.update(task_id, completed=completed, total=total)
        else:
            self.progress.update(task_id, completed=completed)

    def print_summary(self):
        from rich.console import Console
        from rich.table import Table
        from rich.panel import Panel

        # Ensure the progress bar is cleared before printing summary
        self.progress.stop()

        console = Console()
        table = Table(
            show_header=True,
            header_style="bold magenta",
        )
        table.add_column("Status", style="bold")
        table.add_column("Count", style="bold")
        table.add_row("[green]Completed[/green]", str(self.status_counts["done"]))
        table.add_row("[yellow]Skipped[/yellow]", str(self.status_counts["skipped"]))
        table.add_row("[red]Failed[/red]", str(self.status_counts["failed"]))
        # table.add_row(
        #     "[blue]Unknown[/blue]", str(self.status_counts["unknown"])
        # ) # TODO: fix
        table.add_row(
            "[white]Cancelled[/white]",
            str(len(self.files) - sum(self.status_counts.values())),
        )
        console.print(
            Panel(table, title="[white]Download Summary", border_style="white")
        )
        if self.failed_files:
            log_path = get_download_error_log_path()
            if log_path:
                console.print(
                    f"\n[red]{len(self.failed_files)} failed — see {log_path}[/red]"
                )
            else:
                console.print(f"\n[red]{len(self.failed_files)} failed[/red]")
            preview = self.failed_files[:5]
            for file, msg in preview:
                short_msg = msg if len(msg) <= 120 else msg[:117] + "..."
                console.print(f"  [red]• {file.filename}[/red] [dim]({short_msg})[/dim]")
            if len(self.failed_files) > len(preview):
                console.print(
                    f"  [dim]… and {len(self.failed_files) - len(preview)} more in log[/dim]"
                )
