# esgpulllite/esgpulllite/custom/file_watcher.py
import asyncio
import time
from pathlib import Path
from typing import Dict, Set, Optional
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
from rich.console import Console
from concurrent.futures import ProcessPoolExecutor
import threading
import hashlib

from .regrid import RegridderManager, _process_file_chunk_safe


class NetCDFFileHandler(FileSystemEventHandler):
    """Handles file system events for NetCDF files."""
    
    def __init__(self, regrid_processor):
        self.regrid_processor = regrid_processor
        self.console = Console()
        
    def on_created(self, event):
        if not event.is_directory and event.src_path.endswith('.nc'):
            self.regrid_processor.queue_file(Path(event.src_path))
            
    def on_moved(self, event):
        if not event.is_directory and event.src_path.endswith('.nc'):
            self.regrid_processor.queue_file(Path(event.dest_path))


class SubdirectoryRegridManager:
    """Manages regridding weights at the subdirectory level."""
    
    def __init__(self, fs=None, target_res=(1, 1), periodic=True, reuse_weights=True):
        self.fs = fs
        self.target_res = target_res
        self.periodic = periodic
        self.reuse_weights = reuse_weights
        self.regridders: Dict[str, RegridderManager] = {}
        self.console = Console()
    
    def get_subdirectory_key(self, file_path: Path) -> str:
        """Get a unique key for the subdirectory containing the file."""
        # Find the lowest subdirectory that contains .nc files
        current_dir = file_path.parent
        while current_dir != current_dir.parent:  # Stop at root
            # Check if this directory contains .nc files
            if any(current_dir.glob("*.nc*")) and not current_dir.name.startswith("reprojected"):
                return str(current_dir)
            current_dir = current_dir.parent
        return str(file_path.parent)  # Fallback to immediate parent
    
    def get_or_create_regridder(self, file_path: Path) -> RegridderManager:
        """Get or create a RegridderManager for the subdirectory."""
        subdir_key = self.get_subdirectory_key(file_path)
        
        if subdir_key not in self.regridders:
            # Create a new RegridderManager for this subdirectory
            # We'll need to load a sample file to initialize the regridder
            sample_file = self._find_sample_file(Path(subdir_key))
            if sample_file:
                try:
                    import xarray as xa
                    ds = xa.open_dataset(sample_file)
                    # ds = ds.rename({"latitude": "lat", "longitude": "lon"})
                    
                    # Create weight directory specific to this subdirectory
                    weight_dir = Path(subdir_key) / "regrid_weights"
                    weight_dir.mkdir(exist_ok=True)
                    
                    # Create RegridderManager with subdirectory-specific weight directory
                    regridder = RegridderManager(
                        ds=ds,
                        fs=self.fs,
                        target_res=self.target_res,
                        periodic=self.periodic
                    )
                    
                    # Override the weight directory to be subdirectory-specific
                    regridder.weight_dir = weight_dir
                    regridder.weight_dir.mkdir(exist_ok=True)
                    
                    self.regridders[subdir_key] = regridder
                    self.console.print(f"[blue]Created regridder for:[/blue] {subdir_key}")
                    
                except Exception as e:
                    self.console.print(f"[red]Error creating regridder for {subdir_key}:[/red] {e}")
                    return None
            else:
                self.console.print(f"[yellow]No sample file found in:[/yellow] {subdir_key}")
                return None
        
        return self.regridders[subdir_key]
    
    def _find_sample_file(self, subdir: Path) -> Optional[Path]:
        """Find a sample .nc file in the subdirectory to initialize the regridder."""
        nc_files = list(subdir.glob("*.nc*"))
        if nc_files:
            return nc_files[0]
        return None


class AsyncRegridProcessor:
    """Asynchronously processes NetCDF files for regridding with subdirectory-level weight reuse."""
    
    def __init__(
        self,
        watch_dir: Path,
        fs=None,
        target_res=(1, 1),
        max_workers=2,
        batch_size=5,
        batch_timeout=30,
        reuse_weights=True,
        delete_original=False,
    ):
        self.watch_dir = Path(watch_dir)
        self.fs = fs
        self.target_res = target_res
        self.max_workers = max_workers
        self.batch_size = batch_size
        self.batch_timeout = batch_timeout
        self.reuse_weights = reuse_weights
        self.delete_original = delete_original
        self.file_queue = asyncio.Queue()
        self.processed_files: Set[Path] = set()
        self.console = Console()
        self.running = False
        
        # Initialize subdirectory regrid manager
        self.subdir_manager = SubdirectoryRegridManager(
            fs=fs,
            target_res=target_res,
            periodic=True,
            reuse_weights=reuse_weights
        )
        
    def queue_file(self, file_path: Path):
        """Add a file to the processing queue."""
        if file_path not in self.processed_files:
            self.file_queue.put_nowait(file_path)
            self.console.print(f"[blue]Queued:[/blue] {file_path.name}")
    
    def _get_output_path(self, file_path: Path) -> Path:
        """Generate output path for regridded file."""
        parent_dir = file_path.parent
        out_dir = parent_dir / "reprojected"
        out_dir.mkdir(exist_ok=True)
        return out_dir / file_path.name
    
    async def process_batch(self, files: list[Path]):
        """Process a batch of files, grouping by subdirectory for weight reuse."""
        if not files:
            return
            
        # Group files by subdirectory for weight reuse
        subdir_groups: Dict[str, list[Path]] = {}
        for file_path in files:
            subdir_key = self.subdir_manager.get_subdirectory_key(file_path)
            if subdir_key not in subdir_groups:
                subdir_groups[subdir_key] = []
            subdir_groups[subdir_key].append(file_path)
        
        # Process each subdirectory group
        for subdir_key, subdir_files in subdir_groups.items():
            await self._process_subdirectory_batch(subdir_key, subdir_files)
    
    async def _process_subdirectory_batch(self, subdir_key: str, files: list[Path]):
        """Process files from the same subdirectory using shared weights."""
        try:
            # Get or create regridder for this subdirectory
            regridder = self.subdir_manager.get_or_create_regridder(files[0])
            if regridder is None:
                self.console.print(f"[red]Could not create regridder for:[/red] {subdir_key}")
                return
            
            # Create file chunks for parallel processing
            file_chunks = [
                [(f, self._get_output_path(f)) for f in files[i:i+self.batch_size]]
                for i in range(0, len(files), self.batch_size)
            ]
            
            # Use ProcessPoolExecutor for parallel processing
            with ProcessPoolExecutor(max_workers=self.max_workers) as executor:
                loop = asyncio.get_event_loop()
                
                # Submit chunks to workers
                futures = []
                for chunk in file_chunks:
                    future = loop.run_in_executor(
                        executor,
                        _process_file_chunk_safe,
                        chunk,
                        self.fs,
                        self.target_res,
                        True,  # periodic
                        self.reuse_weights,
                        self.delete_original
                    )
                    futures.append(future)
                
                # Wait for all chunks to complete
                results = await asyncio.gather(*futures, return_exceptions=True)
                
                # Process results
                total_success = 0
                total_fail = 0
                for result in results:
                    if isinstance(result, Exception):
                        self.console.print(f"[red]Batch processing error:[/red] {result}")
                        total_fail += len(files)
                    else:
                        success, fail = result
                        total_success += success
                        total_fail += fail
                
                self.console.print(
                    f"[green]Subdirectory {subdir_key}:[/green] "
                    f"Success: {total_success}, Failed: {total_fail}"
                )
                
        except Exception as e:
            self.console.print(f"[red]Error processing subdirectory {subdir_key}:[/red] {e}")
    
    async def process_queue(self):
        """Main processing loop."""
        self.console.print(f"[green]Starting async regrid processor for:[/green] {self.watch_dir}")
        
        while self.running:
            try:
                # Collect files for batch processing
                batch_files = []
                batch_start = time.time()
                
                # Wait for first file
                try:
                    first_file = await asyncio.wait_for(self.file_queue.get(), timeout=1.0)
                    batch_files.append(first_file)
                except asyncio.TimeoutError:
                    continue
                
                # Collect additional files for batch
                while len(batch_files) < self.batch_size:
                    try:
                        file_path = await asyncio.wait_for(
                            self.file_queue.get(), 
                            timeout=self.batch_timeout
                        )
                        batch_files.append(file_path)
                    except asyncio.TimeoutError:
                        break
                
                # Process the batch
                if batch_files:
                    self.console.print(f"[blue]Processing batch of {len(batch_files)} files[/blue]")
                    await self.process_batch(batch_files)
                    
                    # Mark files as processed
                    for file_path in batch_files:
                        self.processed_files.add(file_path)
                        
            except Exception as e:
                self.console.print(f"[red]Error in processing loop:[/red] {e}")
                await asyncio.sleep(5)  # Wait before retrying
                
    async def scan_existing_files(self):
        """Scan for existing files that need regridding."""
        self.console.print(f"[blue]Scanning for existing files in:[/blue] {self.watch_dir}")
        
        existing_files = []
        for ncfile in self.watch_dir.rglob("*.nc*"):
            if "reprojected" in str(ncfile) or "regrid_weights" in str(ncfile):
                continue
            
            # Check if regridded version already exists
            parent_dir = ncfile.parent
            out_dir = parent_dir / "reprojected"
            out_file = out_dir / ncfile.name
            
            if not out_file.exists():
                existing_files.append(ncfile)
        
        if existing_files:
            self.console.print(f"[blue]Found {len(existing_files)} existing files to process[/blue]")
            # Add existing files to queue
            for file_path in existing_files:
                self.file_queue.put_nowait(file_path)
        else:
            self.console.print("[yellow]No existing files need processing[/yellow]")

    
    async def start(self):
        """Start the async regrid processor."""
        self.running = True
        
        await self.scan_existing_files()

        # Start the processing task
        process_task = asyncio.create_task(self.process_queue())
        
        # Start file system watcher
        event_handler = NetCDFFileHandler(self)
        observer = Observer()
        observer.schedule(event_handler, str(self.watch_dir), recursive=True)
        observer.start()
        
        try:
            await process_task
        except KeyboardInterrupt:
            self.console.print("[yellow]Shutting down...[/yellow]")
        finally:
            observer.stop()
            observer.join()
            self.running = False


def start_async_regridder(
    watch_dir: str | Path,
    fs=None,
    target_res=(1, 1),
    max_workers=2,
    batch_size=5,
    batch_timeout=30,
    reuse_weights=True,
    delete_original=False,
    process_existing=False
):
    """Start the asynchronous regridding system."""
    processor = AsyncRegridProcessor(
        watch_dir=Path(watch_dir),
        fs=fs,
        target_res=target_res,
        max_workers=max_workers,
        batch_size=batch_size,
        batch_timeout=batch_timeout,
        reuse_weights=reuse_weights,
        delete_original=delete_original,
    )
    
    if process_existing:
        processor.scan_existing_files()
    
    asyncio.run(processor.start())