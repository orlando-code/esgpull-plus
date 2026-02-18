# general
import threading
import time
from pathlib import Path

# third-party
import requests


class _SkipFileError(Exception):
    """Raised when a file should be skipped (e.g. empty URL), not failed."""

# spatial
import xarray as xa

from esgpull.esgpullplus import ui, fileops
from esgpull.esgpullplus.enhanced_file import EnhancedFile
from esgpull import utils
# parallel
import concurrent.futures


class DownloadSubset:
    def __init__(
        self,
        files,
        fs,
        output_dir=None,
        data_dir=None,
        subset=None,
        max_workers=4,
        force_direct_download=False,
        verbose=False,
        find_alternatives=True,
        api_instance=None,
    ):
        self.files = files
        self.fs = fs
        self.output_dir = output_dir
        self.data_dir = Path(data_dir) if data_dir else None
        self.subset = subset
        self.max_workers = max_workers if max_workers < len(files) else len(files)
        self.force_direct_download = force_direct_download
        self.verbose = verbose
        self.find_alternatives = find_alternatives
        self.api_instance = api_instance
        self._shutdown_requested = threading.Event()

    def _get_file_path(self, file):
        """Get the path to the file on the file system.
        
        Args:
            file (esgpull.esgpullplus.enhanced_file.EnhancedFile): file object to get the path for

        Returns:
            Path: the path to the file on the file system
        """
        if self.output_dir:
            return Path(self.output_dir) / file.filename
        if self.data_dir:
            return self.data_dir / file.local_path / file.filename
        return self.fs.paths.data / file.local_path / file.filename

    def _file_exists(self, file: EnhancedFile) -> bool:
        """Check if the file exists on the file system.
        
        Args:
            file (esgpull.esgpullplus.enhanced_file.EnhancedFile): file object to check if exists

        Returns:
            bool: True if the file exists, False otherwise
        """
        # Need filename for downloads - dataset records don't have it
        filename = getattr(file, "filename", "") or (file.get("filename", "") if isinstance(file, dict) else "")
        if not (filename and str(filename).strip()):
            return False  # Can't download without a filename
        file_path = self._get_file_path(file)
        exists = file_path.is_file() and file_path.stat().st_size > 0
        if not exists:
            self._remove_part_file_detritus(file) # remove any leftover .part files before attempting to download
        return exists
    
    def _remove_part_file_detritus(self, file: EnhancedFile) -> None:
        """Remove any leftover .part files before attempting to download. Avoids issues with partial downloads e.g. overwriting permissions.
        
        Args:
            file (esgpull.esgpullplus.enhanced_file.EnhancedFile): file object to remove the .part file for
        """
        part_file = self._get_file_path(file).with_suffix(self._get_file_path(file).suffix + ".part")
        if part_file.exists():
            try:
                part_file.unlink()
            except Exception:
                pass

    def run(self) -> None:
        """Run the download subset."""
        from rich.console import Console

        console = Console()
        start_time = fileops.print_timestamp(console, "START")

        files_to_download = [f for f in self.files if not self._file_exists(f)]
        if not files_to_download:
            console.print("All files already exist, nothing to download.")
            return

        # get total size of files to download
        total_size = sum(file.size for file in files_to_download)

        dl_str = "file" if len(files_to_download) == 1 else "files"

        console.print(
            f"Downloading {len(files_to_download)} new {dl_str} [APPROX TOTAL: {utils.format_size(total_size)}]..."
        )

        with ui.DownloadProgressUI(files_to_download) as ui_instance:
            with concurrent.futures.ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                futures = [
                    executor.submit(self._process_file_with_ui, file, ui_instance)
                    for file in files_to_download
                ]
                for _ in concurrent.futures.as_completed(futures):
                    if self._shutdown_requested.is_set():
                        break
            ui_instance.print_summary()
        end_time = fileops.print_timestamp(console, "END")
        processing_time = fileops.get_processing_time(start_time, end_time)
        console.print(f"Processing time: {fileops.format_processing_time(processing_time)}")
    

    def _process_file_with_ui(self, file: EnhancedFile, ui_instance: ui.DownloadProgressUI) -> None:
        """Process a file with the downloading progress UI.
        
        Args:
            file (esgpull.esgpullplus.enhanced_file.EnhancedFile): file object to process
            ui_instance (ui.DownloadProgressUI): the downloading progress UI instance to update
        """
        try:
            ui_instance.set_status(file, "STARTING", "cyan")
            try:
                success = self._download_file_direct_ui(file, ui_instance)
            except _SkipFileError as e:
                ui_instance.set_status(file, "SKIPPED", "yellow")
                ui_instance.complete_file(file)
                return
            if success:
                ui_instance.set_status(file, "DONE", "green")
                ui_instance.complete_file(file)
                # Hide the file's progress bar after 5 seconds
                self.hide_file_after_delay(file, ui_instance, 5)
            else:
                # Check if we should try alternatives for timeout errors
                if self.find_alternatives and self.api_instance:
                    if self._try_alternative_file(file, ui_instance):
                        return  # Successfully downloaded alternative
                
                ui_instance.set_status(file, "FAILED", "red")
                ui_instance.complete_file(file)
        except requests.exceptions.Timeout as timeout_error:
            # Handle timeout exception specifically
            if self.find_alternatives and self.api_instance:
                if self._try_alternative_file(file, ui_instance, timeout_error):
                    return  # Successfully downloaded alternative
            
            import traceback
            error_msg = f"Download timeout for {file.filename}: {timeout_error}"
            print(f"\n[ERROR] {error_msg}")
            print(f"[ERROR] URL: {file.url}")
            print(f"[ERROR] File size: {file.size}")
            print(f"[ERROR] Data node: {getattr(file, 'data_node', 'Unknown')}")
            print("-" * 80)
            ui_instance.set_status(file, "TIMEOUT", "red")
            ui_instance.add_failed(file, f"Timeout: {timeout_error}", timeout_error)
            ui_instance.complete_file(file)
            self.hide_file_after_delay(file, ui_instance, 5)
        except Exception as e:
            import traceback
            error_msg = f"Processing failed for {file.filename}: {e}"
            print(f"\n[ERROR] {error_msg}")
            print(f"[ERROR] Full traceback:")
            traceback.print_exc()
            print(f"[ERROR] URL: {file.url}")
            print(f"[ERROR] File size: {file.size}")
            print(f"[ERROR] Data node: {getattr(file, 'data_node', 'Unknown')}")
            print("-" * 80)
            ui_instance.set_status(file, "ERROR", "red")
            ui_instance.add_failed(file, f"Error: {e}", e)
            ui_instance.complete_file(file)
    
    def _try_alternative_file(
        self,
        failed_file: EnhancedFile,
        ui_instance: ui.DownloadProgressUI,
        original_error: Exception = None,
    ) -> bool:
        """
        Try to find and download an alternative file when the original download fails.
        
        Args:
            failed_file: The file that failed to download
            ui_instance: UI instance for progress updates
            original_error: The original error that occurred
            
        Returns:
            True if an alternative was successfully downloaded, False otherwise
        """
        from rich import print as rich_print
        
        # Check if this is a timeout or connection error
        is_timeout = False
        if original_error is not None:
            is_timeout = isinstance(original_error, requests.exceptions.Timeout)
        elif hasattr(failed_file, 'filename'):
            # If no explicit error but download failed, try alternatives anyway
            # (timeout errors might have been caught elsewhere)
            is_timeout = True
        
        # Always try alternatives for failed downloads if enabled
        # The original download already failed, so we should attempt alternatives
        
        try:
            # Convert EnhancedFile to dict for search
            file_dict = failed_file.asdict() if hasattr(failed_file, 'asdict') else {
                'file_id': getattr(failed_file, 'file_id', ''),
                'dataset_id': getattr(failed_file, 'dataset_id', ''),
                'instance_id': getattr(failed_file, 'instance_id', ''),
                'variable': getattr(failed_file, 'variable', ''),
                'variable_id': getattr(failed_file, 'variable_id', ''),
                'experiment_id': getattr(failed_file, 'experiment_id', ''),
                'frequency': getattr(failed_file, 'frequency', ''),
                'time_frequency': getattr(failed_file, 'time_frequency', ''),
                'project': getattr(failed_file, 'project', ''),
                'mip_era': getattr(failed_file, 'mip_era', ''),
                'data_node': getattr(failed_file, 'data_node', ''),
                'version': getattr(failed_file, 'version', ''),
                'filename': getattr(failed_file, 'filename', ''),
            }
            
            # Find alternatives
            alternatives = self.api_instance.find_alternative_files(
                file_dict,
                exclude_file_ids=[file_dict.get('file_id', '')]
            )
            
            if not alternatives:
                return False
            
            # Try each alternative until one succeeds
            for alt_file_dict in alternatives:
                try:
                    rich_print(
                        f"[blue]🔄 Trying alternative file: {alt_file_dict.get('filename', 'unknown')} "
                        f"from {alt_file_dict.get('data_node', 'unknown node')}[/blue]"
                    )
                    
                    # Convert dict back to EnhancedFile
                    from esgpull.esgpullplus.enhanced_file import EnhancedFile
                    alt_file = EnhancedFile.fromdict(alt_file_dict)
                    
                    # Update UI to show we're trying alternative
                    ui_instance.set_status(
                        failed_file,
                        f"TRYING_ALT: {alt_file.filename[:30]}...",
                        "yellow"
                    )
                    
                    # Try downloading alternative
                    alt_success = self._download_file_direct_ui(alt_file, ui_instance)
                    
                    if alt_success:
                        rich_print(
                            f"[green]✅ Successfully downloaded alternative file: "
                            f"{alt_file.filename} from {alt_file.data_node}[/green]"
                        )
                        # Update the original file's status to show it was replaced
                        ui_instance.set_status(
                            failed_file,
                            f"REPLACED: {alt_file.filename[:30]}...",
                            "green"
                        )
                        ui_instance.complete_file(failed_file)
                        return True
                    else:
                        rich_print(
                            f"[yellow]⚠️  Alternative file {alt_file.filename} also failed[/yellow]"
                        )
                        continue
                        
                except Exception as alt_error:
                    rich_print(
                        f"[yellow]⚠️  Error trying alternative {alt_file_dict.get('filename', 'unknown')}: {alt_error}[/yellow]"
                    )
                    continue
            
            return False
            
        except Exception as e:
            rich_print(
                f"[yellow]⚠️  Error in alternative file search: {e}[/yellow]"
            )
            return False

    def _is_direct_download_needed(self) -> bool:
        """Check if direct download should be used."""
        if self.force_direct_download:
            return True
        else:
            return False

    def _download_via_xarray_ui(self, file: EnhancedFile, ui_instance: ui.DownloadProgressUI) -> bool:
        """Download a file via xarray using the downloading progress UI.
        
        Args:
            file (esgpull.esgpullplus.enhanced_file.EnhancedFile): file object to download
            ui_instance (ui.DownloadProgressUI): the downloading progress UI instance to update

        Returns (bool): True if the file was downloaded successfully, False otherwise
        """
        try:
            ui_instance.set_status(file, "OPENING", "cyan")
            ds = self._open_dataset_simple(file)
            if ds is None:
                return False
            if self.subset:
                subset_dims = {
                    k: v
                    for k, v in self.subset.items()
                    if k in ds.dims or k in ds.coords
                }
                if subset_dims:
                    ds = ds.isel(**subset_dims)
            ui_instance.set_status(file, "LOADING", "blue")
            ds.load()
            ui_instance.set_status(file, "SAVING", "yellow")
            return self._save_dataset(file, ds)
        except Exception as e:
            import traceback
            error_msg = f"xarray download failed for {file.filename}: {e}"
            print(f"\n[ERROR] {error_msg}")
            print(f"[ERROR] Full traceback:")
            traceback.print_exc()
            print(f"[ERROR] URL: {file.url}")
            print(f"[ERROR] File size: {file.size}")
            print(f"[ERROR] Data node: {getattr(file, 'data_node', 'Unknown')}")
            print("-" * 80)
            return False

    def _test_url(self, url: str) -> bool:
        """Test if URL is accessible with a HEAD request.
        
        Args:
            url (str): the URL to test

        Returns (bool): True if the URL is accessible, False otherwise
        """
        try:
            response = requests.head(url, timeout=10)
            return response.status_code == 200
        except Exception:
            return False

    def _download_file_direct_ui(self, file: EnhancedFile, ui_instance: ui.DownloadProgressUI, max_retries: int = 3) -> bool:
        """Download a file directly to the file system with resume capability and retry logic.
        
        Supports resuming partial downloads using HTTP Range requests for large files.
        
        Args:
            file (esgpull.esgpullplus.enhanced_file.EnhancedFile): file object to download
            ui_instance (ui.DownloadProgressUI): the downloading progress UI instance to update
            max_retries (int): Maximum number of retry attempts on failure

        Returns (bool): True if the file was downloaded successfully, False otherwise
        """
        timeout_connect = 60
        timeout_read = 600 if (file.size or 0) > 10_000_000_000 else 300
        file_path = self._get_file_path(file)
        temp_path = file_path.with_suffix(file_path.suffix + ".part")
        
        # Check if file already exists
        if temp_path.exists():
            time.sleep(2)
            if file_path.exists() and file_path.stat().st_size > 0:
                return True
        
        # Skip files with empty or invalid URL - raise so caller marks as SKIPPED
        if not file.url or not file.url.startswith("http"):
            raise _SkipFileError(file, "Empty or invalid URL")
        
        file_path.parent.mkdir(parents=True, exist_ok=True)
        
        # Check for existing partial download
        resume_position = 0
        if temp_path.exists():
            resume_position = temp_path.stat().st_size
            if resume_position > 0:
                if self.verbose:
                    print(f"[yellow]Resuming download of {file.filename} from position {resume_position:,} bytes[/yellow]")
        
        # Attempt download with retries
        for attempt in range(max_retries + 1):
            try:
                ui_instance.set_status(file, "DOWNLOADING", "blue")
                
                # Prepare headers for resume if partial file exists
                headers = {}
                if resume_position > 0:
                    headers['Range'] = f'bytes={resume_position}-'
                
                response = requests.get(
                    file.url,
                    stream=True,
                    timeout=(timeout_connect, timeout_read),
                    headers=headers
                )
                
                # Handle partial content response (206) or full content (200)
                if response.status_code == 206:  # Partial Content (resume)
                    total = int(response.headers.get("content-range", "").split("/")[-1])
                    if self.verbose:
                        print(f"[blue]Resuming: {resume_position:,} of {total:,} bytes ({100*resume_position/total:.1f}%)[/blue]")
                elif response.status_code == 200:  # Full content
                    total = int(response.headers.get("content-length", file.size or 0))
                    # If we were trying to resume but got full content, start over
                    if resume_position > 0:
                        if self.verbose:
                            print(f"[yellow]Server doesn't support resume, restarting download[/yellow]")
                        temp_path.unlink()
                        resume_position = 0
                        total = int(response.headers.get("content-length", file.size or 0))
                else:
                    response.raise_for_status()
                
                # Open file in append mode if resuming, write mode otherwise
                mode = "ab" if resume_position > 0 else "wb"
                bytes_downloaded = resume_position
                
                # Set progress bar
                ui_instance.update_file_progress(file, bytes_downloaded, total)
                
                # Download with larger chunk size for better performance on large files
                chunk_size = 65536 if total > 1_000_000_000 else 8192  # 64KB for large files, 8KB for small
                
                with open(temp_path, mode) as f:
                    try:
                        for chunk in response.iter_content(chunk_size=chunk_size):
                            if chunk and not self._shutdown_requested.is_set():
                                f.write(chunk)
                                bytes_downloaded += len(chunk)
                                ui_instance.update_file_progress(file, bytes_downloaded, total)
                    except requests.exceptions.ChunkedEncodingError as e:
                        # Connection broken - check if we got partial data
                        current_size = temp_path.stat().st_size
                        if current_size > resume_position:
                            # Made progress, save position and retry
                            if self.verbose:
                                print(f"[yellow]Connection broken after {current_size:,} bytes. Will retry...[/yellow]")
                            # Continue to retry loop
                            raise
                        else:
                            # No progress made, raise to cleanup
                            raise
                
                # Verify download completed
                if temp_path.exists() and temp_path.stat().st_size > 0:
                    final_size = temp_path.stat().st_size
                    if total > 0 and final_size < total:
                        if self.verbose:
                            print(f"[yellow]Download incomplete: {final_size:,} of {total:,} bytes. Retrying...[/yellow]")
                        if attempt < max_retries:
                            resume_position = final_size
                            time.sleep(2 ** attempt)  # Exponential backoff
                            continue
                        else:
                            print(f"[ERROR] Download incomplete after {max_retries + 1} attempts")
                            return False
                    
                    # Rename to final file
                    temp_path.rename(file_path)
                    if file_path.exists() and file_path.stat().st_size > 0:
                        if self.verbose:
                            print(f"[DEBUG] Successfully downloaded {file.filename} ({file_path.stat().st_size:,} bytes)") 
                        return True
                    else:
                        print(f"[ERROR] Downloaded file {file.filename} is empty or missing after rename")
                        return False
                else:
                    print(f"[ERROR] Downloaded file {file.filename} is empty or missing")
                    return False
                    
            except (requests.exceptions.ChunkedEncodingError, requests.exceptions.ConnectionError, requests.exceptions.Timeout) as e:
                # Handle connection errors with retry
                current_size = temp_path.stat().st_size if temp_path.exists() else 0
                
                if attempt < max_retries:
                    if self.verbose:
                        error_type = type(e).__name__
                        print(f"[yellow]{error_type} on attempt {attempt + 1}/{max_retries + 1} for {file.filename}[/yellow]")
                        print(f"[yellow]Downloaded {current_size:,} bytes so far. Retrying in {2 ** attempt}s...[/yellow]")
                    resume_position = current_size
                    time.sleep(2 ** attempt)  # Exponential backoff: 1s, 2s, 4s
                    continue
                else:
                    # Final attempt failed - this will be caught by _process_file_with_ui
                    # which can then try alternatives if enabled
                    if self.verbose:
                        print(f"[red]Failed after {max_retries + 1} attempts. Partial file saved at {current_size:,} bytes[/red]")
                    # Don't delete partial file - user can resume later or alternatives can be tried
                    raise
                    
            except Exception as e:
                import traceback
                error_msg = f"Direct download failed for {file.filename}: {e}"
                print(f"\n[ERROR] {error_msg}")
                print(f"[ERROR] Full traceback:")
                traceback.print_exc()
                print(f"[ERROR] URL: {file.url}")
                print(f"[ERROR] File size: {file.size}")
                print(f"[ERROR] Data node: {getattr(file, 'data_node', 'Unknown')}")
                
                # Additional debugging for common issues
                if "timeout" in str(e).lower():
                    print(f"[ERROR] This appears to be a timeout issue. Try increasing timeout or check network connection.")
                elif "connection" in str(e).lower() or "chunkedencoding" in str(e).lower():
                    print(f"[ERROR] This appears to be a connection issue. Partial file may be saved for resume.")
                elif "404" in str(e).lower() or "not found" in str(e).lower():
                    print(f"[ERROR] File not found. The URL may be incorrect or the file may have been moved.")
                elif "403" in str(e).lower() or "forbidden" in str(e).lower():
                    print(f"[ERROR] Access forbidden. You may need authentication or the file may be restricted.")
                elif "ssl" in str(e).lower() or "certificate" in str(e).lower():
                    print(f"[ERROR] SSL/Certificate issue. Try updating certificates or using a different data node.")
                
                print("-" * 80)
                
                # Only delete partial file if it's a non-resumable error (404, 403, etc.)
                if "404" in str(e).lower() or "403" in str(e).lower() or "not found" in str(e).lower():
                    for path in [temp_path, file_path]:
                        if path.exists():
                            try:
                                path.unlink()
                            except Exception:
                                pass
                # For connection errors, keep partial file for resume
                
                if attempt >= max_retries:
                    return False
                else:
                    # Continue to retry
                    time.sleep(2 ** attempt)
                    continue
        
        # All retries exhausted
        return False

    def _open_dataset_simple(self, file: EnhancedFile) -> xa.Dataset | None:
        """Simple dataset opening with fallback.
        
        Args:
            file (esgpull.esgpullplus.enhanced_file.EnhancedFile): file object to open

        Returns (xa.Dataset | None): the dataset if opened successfully, None otherwise
        """
        """Simple dataset opening with fallback."""
        engines = ["h5netcdf", "netcdf4"]

        for engine in engines:
            try:
                ds = xa.open_dataset(
                    file.url,
                    engine=engine,
                    chunks={"time": 6},
                    decode_times=False,
                    cache=False,
                )
                return ds
            except Exception:
                try:
                    # Try bytes mode
                    ds = xa.open_dataset(
                        f"{file.url}#mode=bytes",
                        engine=engine,
                        chunks={"time": 6},
                        decode_times=False,
                        cache=False,
                    )
                    return ds
                except Exception:
                    continue

        return None

    def _save_dataset(self, file: EnhancedFile, ds: xa.Dataset) -> bool:
        """Simple dataset saving with attribute cleaning.
        
        Args:
            file (esgpull.esgpullplus.enhanced_file.EnhancedFile): file object to save
            ds (xa.Dataset): the dataset to save

        Returns (bool): True if the dataset was saved successfully, False otherwise
        """
        """Simple dataset saving with attribute cleaning."""
        file_path = self._get_file_path(file)
        temp_path = file_path.with_suffix(file_path.suffix + ".part")

        try:
            # Clean attributes
            ds = self._clean_attributes(ds)

            # Save to temp file
            file_path.parent.mkdir(parents=True, exist_ok=True)
            if temp_path.exists():
                temp_path.unlink()

            ds.to_netcdf(temp_path)

            # Verify and rename
            if temp_path.exists() and temp_path.stat().st_size > 0:
                temp_path.rename(file_path)
                return True

        except Exception as e:
            import traceback
            error_msg = f"Save failed for {file.filename}: {e}"
            print(f"\n[ERROR] {error_msg}")
            print(f"[ERROR] Full traceback:")
            traceback.print_exc()
            print(f"[ERROR] URL: {file.url}")
            print(f"[ERROR] File size: {file.size}")
            print(f"[ERROR] Data node: {getattr(file, 'data_node', 'Unknown')}")
            print("-" * 80)
            # Cleanup
            for path in [temp_path, file_path]:
                if path.exists():
                    try:
                        path.unlink()
                    except Exception:
                        pass

        return False

    def _clean_attributes(self, ds: xa.Dataset) -> xa.Dataset:
        """Clean dataset attributes to prevent encoding errors.
        
        Args:
            ds (xa.Dataset): the dataset to clean

        Returns (xa.Dataset): the cleaned dataset
        """
        """Clean dataset attributes to prevent encoding errors."""

        def safe_str(val):
            if val is None:
                return ""
            try:
                return str(val).encode("utf-8", "replace").decode("utf-8")
            except Exception:
                return ""

        # Clean global attributes
        ds.attrs = {k: safe_str(v) for k, v in ds.attrs.items()}

        # Clean variable attributes
        for var in ds.variables.values():
            var.attrs = {k: safe_str(v) for k, v in var.attrs.items()}

        return ds

    # Add a method to hide file after delay, called from main thread
    def hide_file_after_delay(self, file: EnhancedFile, ui_instance: ui.DownloadProgressUI, delay_seconds: int) -> None:
        """Hide the file after a delay.
        
        Args:
            file (esgpull.esgpullplus.enhanced_file.EnhancedFile): file object to hide from the UI
            ui_instance (ui.DownloadProgressUI): the downloading progress UI instance to update
            delay_seconds (int): the delay in seconds to hide the file
        """
        import threading

        def hide_file():
            time.sleep(delay_seconds)
            try:
                if hasattr(ui_instance, "hide_file"):
                    ui_instance.hide_file(file)
                elif hasattr(ui_instance, "file_task_ids") and hasattr(
                    ui_instance, "progress"
                ):
                    task_id = ui_instance.file_task_ids.get(file.file_id)
                    if task_id is not None:
                        ui_instance.progress.update(task_id, visible=False)
            except Exception:
                pass

        t = threading.Thread(target=hide_file, daemon=True)
        t.start()
