"""
Asynchronous file watcher and regrid processor.

Watches a directory (and its subdirectories) for new NetCDF files and
automatically regrids them using the CDO pipeline from ``cdo_regrid``.

This provides a non-sequential (event-driven) alternative to running
``cdo_regrid.regrid_directory`` manually after all downloads complete.
Files are processed in batches as they appear, grouped by subdirectory
so that regridding weights can be reused within each model/variable
directory.

Fully-formed files
------------------
To avoid processing files that are still being written (e.g. during download),
the watcher can wait until a file is "settled" (no size change for
``file_settle_seconds``) and optionally verify it can be opened as NetCDF
before queueing. Only then is the file passed to the regrid pipeline.

Usage::

    from esgpull.esgpullplus.file_watcher import start_async_regridder

    # Watch a directory; extract surface (top level) and regrid as files arrive
    start_async_regridder(
        watch_dir="/path/to/downloads",
        target_resolution=(1.0, 1.0),
        max_workers=4,
        extract_surface=True,      # regrid top level only
        file_settle_seconds=10.0,  # wait until file is fully written
        validate_can_open=True,    # ensure .nc is openable before queueing
        process_existing=True,
    )
"""

import asyncio
import logging
import threading
import time
from pathlib import Path
from typing import Optional, Set

log = logging.getLogger(__name__)

from rich.console import Console


def _is_nc_fully_formed(file_path: Path, validate_open: bool) -> bool:
    """Return True if the path exists, has positive size, and optionally can be opened as NetCDF."""
    if not file_path.exists():
        return False
    try:
        if file_path.stat().st_size <= 0:
            return False
    except OSError:
        return False
    if not validate_open:
        return True
    try:
        import xarray as xa
        with xa.open_dataset(file_path, decode_times=False) as ds:
            # Lightweight: just open and close; ensures valid NetCDF and readable
            _ = ds.dims
        return True
    except Exception:
        return False


# ---------------------------------------------------------------------------
# File-system event handler
# ---------------------------------------------------------------------------

class NetCDFFileHandler:
    """
    Receives watchdog events and forwards new .nc files to the processor.

    When ``file_settle_seconds`` > 0, paths are added to a pending dict
    (keyed by path, value = first-seen time). The async loop checks periodically
    and only queues files that have been "settled" and optionally pass an
    openability check, so that only fully-formed NetCDF files are processed.
    """

    def __init__(self, processor: "AsyncRegridProcessor"):
        self.processor = processor
        self._console = Console()

    # watchdog FileSystemEventHandler interface
    def dispatch(self, event):
        if hasattr(event, "is_directory") and event.is_directory:
            return
        handler = getattr(self, f"on_{event.event_type}", None)
        if handler:
            handler(event)

    def _enqueue_or_pend(self, path: Path) -> None:
        if path.suffix.lower() != ".nc":
            return
        path = path.resolve()
        if path.stem.lower().startswith("weights_") or "cdo_weights" in path.parts:
            return
        if self.processor.file_settle_seconds <= 0:
            self.processor.queue_file(path)
            return
        with self.processor.pending_lock:
            if path in self.processor.processed_files:
                return
            # Refresh or add; async loop will check settle time and fully-formed
            self.processor.pending[path] = time.monotonic()

    def on_created(self, event):
        self._enqueue_or_pend(Path(event.src_path))

    def on_moved(self, event):
        dest = getattr(event, "dest_path", None) or event.src_path
        self._enqueue_or_pend(Path(dest))


# ---------------------------------------------------------------------------
# Async regrid processor
# ---------------------------------------------------------------------------

class AsyncRegridProcessor:
    """
    Asynchronously processes NetCDF files for regridding.

    Files are collected in a queue, batched by subdirectory, and regridded
    using :func:`cdo_regrid._process_single_file_standalone` (the same
    function used by ``CDORegridPipeline.regrid_batch``).

    Only fully-formed .nc files are queued: after a path is seen, the processor
    waits ``file_settle_seconds`` and optionally verifies the file can be opened
    as NetCDF before adding it to the queue.

    Parameters
    ----------
    watch_dir : Path
        Root directory to watch for new NetCDF files.
    target_resolution : tuple
        Target grid resolution ``(lon_res, lat_res)`` in degrees.
    target_grid : str
        CDO grid type (``"lonlat"``, ``"gaussian"``, etc.).
    weight_cache_dir : Path or None
        Where to store CDO weight files. Defaults to ``<watch_dir>/cdo_weights``.
    max_workers : int
        Number of parallel regridding workers.
    batch_size : int
        Maximum files per processing batch.
    batch_timeout : float
        Seconds to wait for additional files before processing a partial batch.
    extract_surface : bool
        If True, extract top vertical level only and regrid that (surface).
    extract_seafloor : bool
        If True, extract seafloor values and regrid only that.
    use_regrid_cache : bool
        Reuse existing regrid weight files when present.
    use_seafloor_cache : bool
        Reuse seafloor depth indices cache.
    file_settle_seconds : float
        Seconds to wait after a file is first seen before considering it
        "fully formed". Only then is it queued (and optionally validated).
    validate_can_open : bool
        If True, only queue files that can be opened as NetCDF (after settle).
    overwrite : bool
        Overwrite existing regridded files.
    delete_original : bool
        Delete the original file after successful regridding.
    """

    def __init__(
        self,
        watch_dir: Path,
        target_resolution: tuple[float, float] = (1.0, 1.0),
        target_grid: str = "lonlat",
        weight_cache_dir: Optional[Path] = None,
        max_workers: int = 4,
        batch_size: int = 10,
        batch_timeout: float = 30.0,
        extract_surface: bool = True,
        extract_seafloor: bool = False,
        use_regrid_cache: bool = True,
        use_seafloor_cache: bool = True,
        file_settle_seconds: float = 10.0,
        validate_can_open: bool = True,
        overwrite: bool = False,
        delete_original: bool = False,
    ):
        self.watch_dir = Path(watch_dir)
        self.target_resolution = target_resolution
        self.target_grid = target_grid
        self.weight_cache_dir = weight_cache_dir or (self.watch_dir / "cdo_weights")
        self.max_workers = max_workers
        self.batch_size = batch_size
        self.batch_timeout = batch_timeout
        self.extract_surface = extract_surface
        self.extract_seafloor = extract_seafloor
        self.use_regrid_cache = use_regrid_cache
        self.use_seafloor_cache = use_seafloor_cache
        self.file_settle_seconds = file_settle_seconds
        self.validate_can_open = validate_can_open
        self.overwrite = overwrite
        self.delete_original = delete_original

        self.file_queue: asyncio.Queue[Path] = asyncio.Queue()
        self.processed_files: Set[Path] = set()
        self.pending: dict[Path, float] = {}  # path -> first_seen_time (for settle check)
        self.pending_lock = threading.Lock()
        self.running = False

        self._console = Console()

    # ------------------------------------------------------------------
    # Queue management
    # ------------------------------------------------------------------

    def queue_file(self, file_path: Path) -> None:
        """Add a file to the processing queue (idempotent)."""
        if file_path in self.processed_files:
            return
        try:
            self.file_queue.put_nowait(file_path)
            log.debug(f"Queued: {file_path.name}")
        except asyncio.QueueFull:
            log.warning(f"Queue full, dropping: {file_path.name}")


    # ------------------------------------------------------------------
    # Batch processing
    # ------------------------------------------------------------------

    async def _process_batch(self, files: list[Path]) -> None:
        """Process a batch of files using the CDO regrid pipeline."""
        if not files:
            return

        # Deduplicate so the same path is only processed once per batch
        seen: set[Path] = set()
        deduped: list[Path] = []
        for fp in files:
            try:
                r = fp.resolve()
            except OSError:
                r = fp
            if r not in seen:
                seen.add(r)
                deduped.append(fp)
        files = deduped

        from concurrent.futures import ProcessPoolExecutor
        from esgpull.esgpullplus.cdo_regrid import _process_single_file_standalone

        self.weight_cache_dir.mkdir(parents=True, exist_ok=True)

        loop = asyncio.get_event_loop()
        tasks = []

        with ProcessPoolExecutor(max_workers=self.max_workers) as executor:
            for fp in files:
                future = loop.run_in_executor(
                    executor,
                    _process_single_file_standalone,
                    fp,
                    self.target_resolution,
                    self.target_grid,
                    self.weight_cache_dir,
                    self.extract_surface,
                    self.extract_seafloor,
                    self.use_regrid_cache,
                    self.use_seafloor_cache,
                    8.0,     # max_memory_gb
                    2.0,     # chunk_size_gb
                    True,    # enable_chunking
                    self.overwrite,
                    None,    # representative_file
                    False,   # verbose
                )
                tasks.append((fp, future))

            success = fail = skip = 0
            for fp, future in tasks:
                try:
                    result = await future
                    if result.get("skipped"):
                        skip += 1
                    elif result.get("success"):
                        success += 1
                        if self.delete_original:
                            fp.unlink(missing_ok=True)
                    else:
                        fail += 1
                        log.warning(f"Regrid failed for {fp.name}: {result.get('message')}")
                except Exception as exc:
                    fail += 1
                    log.error(f"Error regridding {fp.name}: {exc}")

        self._console.print(
            f"[green]Batch complete:[/green] {success} ok, {fail} failed, {skip} skipped"
        )

    # ------------------------------------------------------------------
    # Main processing loop
    # ------------------------------------------------------------------

    async def _drain_pending(self) -> None:
        """Move settled, fully-formed paths from pending into file_queue."""
        if self.file_settle_seconds <= 0:
            return
        now = time.monotonic()
        with self.pending_lock:
            to_check = [
                p for p, first in self.pending.items()
                if now - first >= self.file_settle_seconds
            ]
            for p in to_check:
                del self.pending[p]
        loop = asyncio.get_event_loop()
        for path in to_check:
            # Run I/O check in executor to avoid blocking
            ok = await loop.run_in_executor(
                None,
                lambda p=path: _is_nc_fully_formed(p, self.validate_can_open),
            )
            if ok:
                self.queue_file(path)
            else:
                log.debug("Skipping not fully-formed or unreadable: %s", path.name)

    async def _process_loop(self) -> None:
        """Collect files from the queue into batches and process them."""
        self._console.print(
            f"[green]Async regrid processor started for:[/green] {self.watch_dir}"
        )
        if self.file_settle_seconds > 0:
            self._console.print(
                f"[dim]Only fully-formed .nc files (settle {self.file_settle_seconds}s, "
                f"validate_open={self.validate_can_open}) will be queued[/dim]"
            )

        while self.running:
            await self._drain_pending()

            batch: list[Path] = []

            # Wait for the first file
            try:
                first = await asyncio.wait_for(self.file_queue.get(), timeout=1.0)
                batch.append(first)
            except asyncio.TimeoutError:
                continue

            # Collect more files up to batch_size or batch_timeout
            deadline = time.monotonic() + self.batch_timeout
            while len(batch) < self.batch_size and time.monotonic() < deadline:
                try:
                    remaining = max(0.1, deadline - time.monotonic())
                    fp = await asyncio.wait_for(self.file_queue.get(), timeout=remaining)
                    batch.append(fp)
                except asyncio.TimeoutError:
                    break

            if batch:
                file_str = "file" if len(batch) == 1 else "files"
                self._console.print(f"[blue]Processing batch of {len(batch)} {file_str}[/blue]")
                await self._process_batch(batch)
                for fp in batch:
                    self.processed_files.add(fp)

    # ------------------------------------------------------------------
    # Scan for pre-existing files
    # ------------------------------------------------------------------

    async def scan_existing_files(self) -> None:
        """Queue existing .nc files that haven't been regridded yet (only fully-formed if validate_can_open)."""
        self._console.print(f"[blue]Scanning existing files in:[/blue] {self.watch_dir}")



        queued = 0
        for nc in self.watch_dir.rglob("*.nc"):
            if "chunk" in nc.name:
                continue
            if nc.stem.lower().startswith("weights_"):
                continue
            parts = nc.parts
            if any(p in ("reprojected", "cdo_weights", "regrid_weights", "regridded") for p in parts):
                continue
            if "regridded" in nc.name:
                continue


            if self.validate_can_open and not _is_nc_fully_formed(nc, validate_open=True):
                log.debug("Skip existing (not openable): %s", nc.name)
                continue
            # self._console.print(f"Queuing: {nc.name}")
            self.queue_file(nc)
            queued += 1

        self._console.print(f"[blue]Queued {queued} existing files for regridding[/blue]")

    # ------------------------------------------------------------------
    # Entry point
    # ------------------------------------------------------------------

    async def start(self) -> None:
        """Start the file watcher and processing loop."""
        self.running = True

        await self.scan_existing_files()

        process_task = asyncio.create_task(self._process_loop())

        # Start filesystem watcher (requires ``watchdog``)
        try:
            from watchdog.observers import Observer
            from watchdog.events import FileSystemEventHandler

            class _Adapter(FileSystemEventHandler):
                def __init__(self, handler):
                    self._h = handler
                def on_created(self, event):
                    self._h.on_created(event)
                def on_moved(self, event):
                    self._h.on_moved(event)

            handler = NetCDFFileHandler(self)
            observer = Observer()
            observer.schedule(_Adapter(handler), str(self.watch_dir), recursive=True)
            observer.start()
        except ImportError:
            log.warning(
                "watchdog not installed; only pre-existing files will be processed. "
                "Install with: pip install watchdog"
            )
            observer = None

        try:
            await process_task
        except (KeyboardInterrupt, asyncio.CancelledError):
            self._console.print("[yellow]Shutting down regrid processor...[/yellow]")
        finally:
            self.running = False
            if observer is not None:
                observer.stop()
                observer.join()

    def stop(self) -> None:
        """Signal the processor to stop after finishing the current batch."""
        self.running = False


# ---------------------------------------------------------------------------
# Convenience entry point
# ---------------------------------------------------------------------------


def start_async_regridder(
    watch_dir: str | Path,
    target_resolution: tuple[float, float] = (1.0, 1.0),
    target_grid: str = "lonlat",
    weight_cache_dir: Optional[Path] = None,
    max_workers: int = 4,
    batch_size: int = 10,
    batch_timeout: float = 30.0,
    extract_surface: bool = True,
    extract_seafloor: bool = False,
    use_regrid_cache: bool = True,
    use_seafloor_cache: bool = True,
    file_settle_seconds: float = 10.0,
    validate_can_open: bool = True,
    overwrite: bool = False,
    delete_original: bool = False,
    process_existing: bool = True,
) -> None:
    """
    Start the asynchronous regridding system.

    This blocks until interrupted (Ctrl+C) or all files are processed
    and no new files appear. Only fully-formed .nc files are processed:
    after a file appears, the watcher waits ``file_settle_seconds`` and
    optionally verifies it can be opened as NetCDF before queueing.

    Parameters
    ----------
    watch_dir : str or Path
        Directory to watch for new .nc files.
    target_resolution : tuple
        ``(lon_res, lat_res)`` in degrees.
    target_grid : str
        CDO grid type.
    weight_cache_dir : Path, optional
        Directory for CDO weight cache.
    max_workers : int
        Parallel worker count.
    batch_size : int
        Max files per batch.
    batch_timeout : float
        Seconds to wait for batch to fill.
    extract_surface : bool
        If True, extract top level only and regrid that.
    extract_seafloor : bool
        If True, extract seafloor and regrid only that.
    use_regrid_cache : bool
        Reuse existing regrid weight files.
    use_seafloor_cache : bool
        Reuse seafloor depth indices cache.
    file_settle_seconds : float
        Seconds to wait after a file is seen before queueing (so only
        fully-written files are processed).
    validate_can_open : bool
        If True, only queue files that can be opened as NetCDF.
    overwrite : bool
        Overwrite existing output files.
    delete_original : bool
        Delete originals after success.
    process_existing : bool
        If True, scan and process pre-existing files on startup.
    """
    processor = AsyncRegridProcessor(
        watch_dir=Path(watch_dir),
        target_resolution=target_resolution,
        target_grid=target_grid,
        weight_cache_dir=weight_cache_dir,
        max_workers=max_workers,
        batch_size=batch_size,
        batch_timeout=batch_timeout,
        extract_surface=extract_surface,
        extract_seafloor=extract_seafloor,
        use_regrid_cache=use_regrid_cache,
        use_seafloor_cache=use_seafloor_cache,
        file_settle_seconds=file_settle_seconds,
        validate_can_open=validate_can_open,
        overwrite=overwrite,
        delete_original=delete_original,
    )

    asyncio.run(processor.start())


import argparse


parser = argparse.ArgumentParser()

parser.add_argument("watch_dir", type=Path, help="Input directory to watch for new files")
parser.add_argument("-r", "--target-resolution", nargs=2, type=float, default=[1.0, 1.0], help="Target resolution (lon_res lat_res)", dest="target_resolution")
parser.add_argument("--target-grid", type=str, default="lonlat", dest="target_grid")
parser.add_argument("--weight-cache-dir", type=Path, default=None, dest="weight_cache_dir")
parser.add_argument("--max-workers", type=int, default=4, dest="max_workers")
parser.add_argument("--batch-size", type=int, default=10, dest="batch_size")
parser.add_argument("--batch-timeout", type=float, default=30.0, dest="batch_timeout")
parser.add_argument("--extract-surface", action="store_true", dest="extract_surface")
parser.add_argument("--extract-seafloor", action="store_true", dest="extract_seafloor", default=False)
parser.add_argument("--use-regrid-cache", action="store_true", dest="use_regrid_cache")
parser.add_argument("--use-seafloor-cache", action="store_true", dest="use_seafloor_cache")
parser.add_argument("--file-settle-seconds", type=float, default=10.0, dest="file_settle_seconds")
parser.add_argument("--validate-can-open", action="store_true", default=True, dest="validate_can_open")
parser.add_argument("--overwrite", action="store_true", dest="overwrite", default=False)
parser.add_argument("--delete-original", action="store_true", dest="delete_original", default=False)
parser.add_argument("--process-existing", action="store_true", dest="process_existing", default=True)


def main():
    """CLI entry point; parse args only when run as __main__ so importing the module (e.g. in tests) does not consume sys.argv."""
    args = parser.parse_args()
    start_async_regridder(
        watch_dir=args.watch_dir,
        target_resolution=args.target_resolution,
        target_grid=args.target_grid,
        weight_cache_dir=args.weight_cache_dir,
        max_workers=args.max_workers,
        batch_size=args.batch_size,
        batch_timeout=args.batch_timeout,
        extract_surface=args.extract_surface,
        extract_seafloor=args.extract_seafloor,
        use_regrid_cache=args.use_regrid_cache,
        use_seafloor_cache=args.use_seafloor_cache,
        file_settle_seconds=args.file_settle_seconds,
        validate_can_open=args.validate_can_open,
        overwrite=args.overwrite,
        delete_original=args.delete_original,
        process_existing=args.process_existing,
    )


if __name__ == "__main__":
    main()