#!/usr/bin/env python3
"""
Rich UI components for CDO regridding pipeline.

This module provides comprehensive progress tracking and status display
for regridding operations, similar to the download UI system.
"""

import time
import logging
from datetime import datetime
from pathlib import Path
from typing import Optional, Dict, List, Any
from concurrent.futures import Future

from rich.progress import (
    Progress,
    BarColumn,
    TimeElapsedColumn,
    TimeRemainingColumn,
    TextColumn,
    SpinnerColumn,
    TaskProgressColumn,
    MofNCompleteColumn,
)
from rich.console import Console
from rich.table import Table
from rich.panel import Panel
from rich.live import Live
from rich.layout import Layout
from rich.text import Text

from esgpull.esgpullplus import config


class RegridProgressUI:
    """Compact progress UI for regridding operations with minimal visual clutter."""

    def __init__(self, files: List[Path], verbose: bool = True):
        self.files = files
        self.verbose = verbose
        self.console = Console()
        
        # Status tracking
        self.status_counts = {
            "completed": 0,
            "skipped": 0,
            "failed": 0,
        }
        
        # File tracking
        self.file_status: Dict[Path, str] = {}
        self.file_task_ids: Dict[Path, Optional[int]] = {}  # Track task IDs for each file
        self.failed_files: List[tuple[Path, str]] = []
        self.processing_stats: Dict[str, Any] = {
            "weights_reused": 0,
            "weights_generated": 0,
            "chunks_processed": 0,
            "total_size_gb": 0.0,
            "memory_peak_gb": 0.0,
        }
        
        # Compact display
        self.overall_task: Optional[int] = None
        self.current_files: List[Path] = []  # Currently processing files
        self.max_concurrent_display = 3  # Max files to show at once
        
        self._setup_logger()
        self._setup_progress()

    def _setup_logger(self):
        """Set up logging for regridding errors."""
        self.logger = logging.getLogger(f"regrid_errors_{id(self)}")
        self.logger.setLevel(logging.ERROR)

        # Create logs directory
        log_dir = config.log_dir
        log_dir.mkdir(exist_ok=True)

        timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
        log_file = log_dir / f"regrid_errors_{timestamp}.log"

        handler = logging.FileHandler(log_file)
        formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
        handler.setFormatter(formatter)

        # Clear existing handlers and add the new one
        if self.logger.hasHandlers():
            self.logger.handlers.clear()
        self.logger.addHandler(handler)

    def _setup_progress(self):
        """Set up compact progress display components."""
        self.progress = Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            BarColumn(),
            TaskProgressColumn(),
            MofNCompleteColumn(),
            TimeElapsedColumn(),
            TimeRemainingColumn(),
            transient=False,
            expand=True,
            auto_refresh=True,
            refresh_per_second=2,  # Reduced refresh rate
        )

    def __enter__(self):
        """Enter the progress context."""
        self.progress.__enter__()
        
        # Add overall progress task
        file_str = "files" if len(self.files) > 1 else "file"
        self.overall_task = self.progress.add_task(
            f"[cyan]Regridding {len(self.files)} {file_str}[/cyan]",
            total=len(self.files)
        )
        
        # Initialize file statuses
        for file_path in self.files:
            self.file_status[file_path] = "PENDING"
        
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Exit the progress context."""
        self.progress.__exit__(exc_type, exc_val, exc_tb)

    def start_file_processing(self, file_path: Path, file_info: Dict[str, Any]):
        """Start processing a file with compact display."""
        self.current_files.append(file_path)
        self.file_status[file_path] = "PROCESSING"
        
        # Only show detailed info for first few files or if verbose
        if len(self.current_files) <= self.max_concurrent_display or self.verbose:
            filename = file_path.name
            size_gb = file_info.get('file_size_gb', 0)
            grid_type = file_info.get('grid_type', 'unknown')
            
            # Create compact task description
            desc = f"[blue]{filename}[/blue] ({size_gb:.1f}GB, {grid_type})"
            task_id = self.progress.add_task(desc, total=100)
            self.file_task_ids[file_path] = task_id
        else:
            # For many files, just track without individual progress bars
            self.file_task_ids[file_path] = None

    def update_file_progress(self, file_path: Path, progress: int, operation: str = ""):
        """Update progress for a specific file."""
        if file_path in self.file_task_ids and self.file_task_ids[file_path] is not None:
            task_id = self.file_task_ids[file_path]
            filename = file_path.name
            desc = f"[blue]{operation}: {filename}[/blue]" if operation else f"[blue]{filename}[/blue]"
            self.progress.update(task_id, completed=progress, description=desc)

    def complete_file(self, file_path: Path, success: bool = True, message: str = ""):
        """Mark a file as completed with compact display."""
        # Remove from current processing
        if file_path in self.current_files:
            self.current_files.remove(file_path)
        
        # Update status
        if success:
            self.file_status[file_path] = "COMPLETED"
            self.status_counts["completed"] += 1
        else:
            self.file_status[file_path] = "FAILED"
            self.status_counts["failed"] += 1
            if message:
                self.failed_files.append((file_path, message))
                self.logger.error(f"File: {file_path.name} - {message}")
        
        # Update individual progress bar if it exists
        if file_path in self.file_task_ids and self.file_task_ids[file_path] is not None:
            task_id = self.file_task_ids[file_path]
            filename = file_path.name
            if success:
                self.progress.update(
                    task_id,
                    completed=100,
                    description=f"[green]✓ {filename}[/green]"
                )
            else:
                self.progress.update(
                    task_id,
                    completed=100,
                    description=f"[red]✗ {filename}[/red]"
                )
            # Hide after short delay
            self._hide_file_after_delay(file_path, 1)
        
        # Advance overall progress
        if self.overall_task is not None:
            self.progress.advance(self.overall_task)
            
        # Update overall progress description with current stats
        self._update_overall_description()

    def skip_file(self, file_path: Path, reason: str = "Already exists"):
        """Mark a file as skipped."""
        # Remove from current processing
        if file_path in self.current_files:
            self.current_files.remove(file_path)
        
        self.file_status[file_path] = "SKIPPED"
        self.status_counts["skipped"] += 1
        
        # Update individual progress bar if it exists
        if file_path in self.file_task_ids and self.file_task_ids[file_path] is not None:
            task_id = self.file_task_ids[file_path]
            filename = file_path.name
            self.progress.update(
                task_id,
                completed=100,
                description=f"[yellow]⏭ {filename}[/yellow]"
            )
            # Hide after short delay
            self._hide_file_after_delay(file_path, 1)
        
        # Advance overall progress
        if self.overall_task is not None:
            self.progress.advance(self.overall_task)
            
        # Update overall progress description
        self._update_overall_description()

    def _update_overall_description(self):
        """Update the overall progress description with current statistics."""
        if self.overall_task is not None:
            total = len(self.files)
            completed = self.status_counts["completed"]
            skipped = self.status_counts["skipped"]
            failed = self.status_counts["failed"]
            processing = len(self.current_files)
            
            desc = f"[cyan]Regridding {total} files[/cyan] - "
            desc += f"[green]✓{completed}[/green] "
            desc += f"[yellow]⏭{skipped}[/yellow] "
            desc += f"[red]✗{failed}[/red]"
            if processing > 0:
                desc += f" [blue]Processing {processing}[/blue]"
            
            self.progress.update(self.overall_task, description=desc)

    def update_chunking_progress(self, file_path: Path, chunk_num: int, total_chunks: int):
        """Update progress for chunked file processing."""
        if file_path in self.file_task_ids and self.file_task_ids[file_path] is not None:
            task_id = self.file_task_ids[file_path]
            progress = int((chunk_num / total_chunks) * 100)
            filename = file_path.name
            self.progress.update(
                task_id,
                completed=progress,
                description=f"[cyan]Chunking {filename} ({chunk_num}/{total_chunks})[/cyan]"
            )

    def update_regridding_progress(self, file_path: Path, operation: str):
        """Update progress for regridding operations."""
        if file_path in self.file_task_ids and self.file_task_ids[file_path] is not None:
            task_id = self.file_task_ids[file_path]
            filename = file_path.name
            self.progress.update(
                task_id,
                description=f"[magenta]{operation}: {filename}[/magenta]"
            )

    def _update_stats(self, stats: Dict[str, Any]):
        """Update processing statistics."""
        self.processing_stats.update(stats)


    def _hide_file_after_delay(self, file_path: Path, delay_seconds: int):
        """Hide file progress bar after a delay."""
        import threading
        
        def hide_file():
            time.sleep(delay_seconds)
            try:
                if file_path in self.file_task_ids and self.file_task_ids[file_path] is not None:
                    task_id = self.file_task_ids[file_path]
                    self.progress.update(task_id, visible=False)
            except Exception:
                pass
        
        thread = threading.Thread(target=hide_file, daemon=True)
        thread.start()

    def print_summary(self):
        """Print a comprehensive summary of the regridding operation."""
        # Stop the progress display
        self.progress.stop()
        
        # Create summary table
        table = Table(show_header=True, header_style="bold magenta")
        table.add_column("Status", style="bold")
        table.add_column("Count", style="bold")
        table.add_column("Percentage", style="bold")
        
        total_files = len(self.files)
        completed = self.status_counts["completed"]
        skipped = self.status_counts["skipped"]
        failed = self.status_counts["failed"]
        
        table.add_row("[green]Completed[/green]", str(completed), f"{(completed/total_files*100):.1f}%")
        table.add_row("[yellow]Skipped[/yellow]", str(skipped), f"{(skipped/total_files*100):.1f}%")
        table.add_row("[red]Failed[/red]", str(failed), f"{(failed/total_files*100):.1f}%")
        
        # Add processing statistics
        stats_table = Table(show_header=True, header_style="bold cyan")
        stats_table.add_column("Metric", style="bold")
        stats_table.add_column("Value", style="bold")
        
        stats_table.add_row("Weights Reused", str(self.processing_stats["weights_reused"]))
        stats_table.add_row("Weights Generated", str(self.processing_stats["weights_generated"]))
        stats_table.add_row("Chunks Processed", str(self.processing_stats["chunks_processed"]))
        stats_table.add_row("Total Size (GB)", f"{self.processing_stats['total_size_gb']:.2f}")
        stats_table.add_row("Peak Memory (GB)", f"{self.processing_stats['memory_peak_gb']:.2f}")
        
        # Add timing information if available
        if "processing_time" in self.processing_stats:
            stats_table.add_row("Processing Time", self.processing_stats["processing_time"])
        
        # Display summary
        self.console.print(Panel(table, title="[bold]Regridding Summary[/bold]", border_style="green"))
        self.console.print(Panel(stats_table, title="[bold]Processing Statistics[/bold]", border_style="cyan"))
        
        # Show failed files if any
        if self.failed_files:
            self._display_failed_files()

    def _display_failed_files(self):
        """Display information about failed files."""
        self.console.print(f"\n[red]Failed Files ({len(self.failed_files)}):[/red]")
        
        for i, (file_path, error_msg) in enumerate(self.failed_files):
            self.console.print(f"[red]{i+1}. {file_path.name}[/red]")
            self.console.print(f"   [red]Error: {error_msg}[/red]")
            self.console.print(f"   [red]Path: {file_path}[/red]")
            self.console.print("")
        
        # Show detailed failure info for first failed file
        if self.failed_files:
            file_path, error_msg = self.failed_files[0]
            self.console.print(
                Panel(
                    f"First failed file: [bold]{file_path.name}[/bold]\n[red]{error_msg}[/red]\n\nPath: {file_path}",
                    title="[red]Detailed Failure Info[/red]",
                    style="red",
                )
            )


class BatchRegridUI:
    """Compact UI for batch regridding operations with parallel processing support."""

    def __init__(self, files: List[Path], max_workers: int = 4, verbose: bool = True):
        self.files = files
        self.max_workers = max_workers
        self.verbose = verbose
        self.console = Console()
        
        # Progress tracking
        self.overall_progress: Optional[int] = None
        self.completed_files: List[Path] = []
        self.failed_files: List[tuple[Path, str]] = []
        self.skipped_files: List[Path] = []
        
        # Statistics
        self.stats = {
            "files_processed": 0,
            "weights_reused": 0,
            "weights_generated": 0,
            "chunks_processed": 0,
            "errors": 0,
            "total_size_gb": 0.0,
            "memory_peak_gb": 0.0,
        }
        
        self._setup_progress()

    def _setup_progress(self):
        """Set up compact progress display for batch operations."""
        self.progress = Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            BarColumn(),
            TaskProgressColumn(),
            MofNCompleteColumn(),
            TimeElapsedColumn(),
            TimeRemainingColumn(),
            transient=False,
            expand=True,
            auto_refresh=True,
            refresh_per_second=2,  # Reduced refresh rate
        )

    def __enter__(self):
        """Enter the batch progress context."""
        self.progress.__enter__()
        
        # Add overall progress task with worker info
        desc = f"[cyan]Batch Regridding {len(self.files)} files ({self.max_workers} workers)[/cyan]"
        self.overall_progress = self.progress.add_task(desc, total=len(self.files))
        
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Exit the batch progress context."""
        self.progress.__exit__(exc_type, exc_val, exc_tb)

    def update_file_result(self, file_path: Path, result: Dict[str, Any]):
        """Update progress based on file processing result."""
        if result.get('success', False):
            if result.get('skipped', False):
                self.skipped_files.append(file_path)
            else:
                self.completed_files.append(file_path)
        else:
            error_msg = result.get('message', 'Unknown error')
            self.failed_files.append((file_path, error_msg))
        
        # Advance progress
        self.progress.advance(self.overall_progress)
        
        # Update overall description with current stats
        self._update_overall_description()
        
        # Update statistics
        if 'stats' in result:
            self._update_stats(result['stats'])

    def _update_overall_description(self):
        """Update the overall progress description with current statistics."""
        if self.overall_progress is not None:
            total = len(self.files)
            completed = len(self.completed_files)
            skipped = len(self.skipped_files)
            failed = len(self.failed_files)
            remaining = total - completed - skipped - failed
            
            desc = f"[cyan]Batch Regridding {total} files ({self.max_workers} workers)[/cyan] - "
            desc += f"[green]✓{completed}[/green] "
            desc += f"[yellow]⏭{skipped}[/yellow] "
            desc += f"[red]✗{failed}[/red]"
            if remaining > 0:
                desc += f" [blue]Remaining: {remaining}[/blue]"
            
            self.progress.update(self.overall_progress, description=desc)

    def _update_stats(self, new_stats: Dict[str, Any]):
        """Update cumulative statistics."""
        for key, value in new_stats.items():
            if key in self.stats:
                if isinstance(value, (int, float)):
                    self.stats[key] += value
                else:
                    self.stats[key] = value

    def print_summary(self):
        """Print compact batch processing summary."""
        self.progress.stop()
        
        # Create compact summary
        table = Table(show_header=True, header_style="bold magenta")
        table.add_column("Status", style="bold")
        table.add_column("Count", style="bold")
        table.add_column("Percentage", style="bold")
        
        total = len(self.files)
        completed = len(self.completed_files)
        skipped = len(self.skipped_files)
        failed = len(self.failed_files)
        
        table.add_row("[green]Completed[/green]", str(completed), f"{(completed/total*100):.1f}%")
        table.add_row("[yellow]Skipped[/yellow]", str(skipped), f"{(skipped/total*100):.1f}%")
        table.add_row("[red]Failed[/red]", str(failed), f"{(failed/total*100):.1f}%")
        
        # Add processing stats
        stats_table = Table(show_header=True, header_style="bold cyan")
        stats_table.add_column("Metric", style="bold")
        stats_table.add_column("Value", style="bold")
        
        stats_table.add_row("Weights Reused", str(self.stats["weights_reused"]))
        stats_table.add_row("Weights Generated", str(self.stats["weights_generated"]))
        stats_table.add_row("Chunks Processed", str(self.stats["chunks_processed"]))
        stats_table.add_row("Total Size (GB)", f"{self.stats['total_size_gb']:.2f}")
        stats_table.add_row("Peak Memory (GB)", f"{self.stats['memory_peak_gb']:.2f}")
        
        # Add timing information if available
        if "processing_time" in self.stats:
            stats_table.add_row("Processing Time", self.stats["processing_time"])
        
        self.console.print(Panel(table, title="[bold]Batch Regridding Summary[/bold]", border_style="green"))
        self.console.print(Panel(stats_table, title="[bold]Processing Statistics[/bold]", border_style="cyan"))
        
        # Show failed files if any (compact)
        if self.failed_files:
            self.console.print(f"\n[red]Failed Files ({len(self.failed_files)}):[/red]")
            for i, (file_path, error_msg) in enumerate(self.failed_files[:5]):  # Show only first 5
                self.console.print(f"[red]{i+1}. {file_path.name} - {error_msg}[/red]")
            if len(self.failed_files) > 5:
                self.console.print(f"[red]... and {len(self.failed_files) - 5} more[/red]")


class ChunkingProgressUI:
    """UI for chunked file processing operations."""

    def __init__(self, file_path: Path, total_chunks: int, verbose: bool = True):
        self.file_path = file_path
        self.total_chunks = total_chunks
        self.verbose = verbose
        self.console = Console()
        
        self._setup_progress()

    def _setup_progress(self):
        """Set up progress display for chunking operations."""
        self.progress = Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            BarColumn(),
            TaskProgressColumn(),
            MofNCompleteColumn(),
            TimeElapsedColumn(),
            TimeRemainingColumn(),
            transient=True,
            expand=True,
            auto_refresh=True,
            refresh_per_second=4,
        )

    def __enter__(self):
        """Enter the chunking progress context."""
        self.progress.__enter__()
        
        # Add chunking progress task
        self.chunking_task = self.progress.add_task(
            f"[cyan]Chunking: {self.file_path.name}[/cyan]",
            total=self.total_chunks
        )
        
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Exit the chunking progress context."""
        self.progress.__exit__(exc_type, exc_val, exc_tb)

    def update_chunk_progress(self, chunk_num: int, operation: str = "Processing"):
        """Update progress for chunk processing."""
        self.progress.update(
            self.chunking_task,
            completed=chunk_num,
            description=f"[cyan]{operation} chunk {chunk_num}/{self.total_chunks}: {self.file_path.name}[/cyan]"
        )

    def complete_chunking(self):
        """Mark chunking as complete."""
        self.progress.update(
            self.chunking_task,
            completed=self.total_chunks,
            description=f"[green]✓ Chunking complete: {self.file_path.name}[/green]"
        )
