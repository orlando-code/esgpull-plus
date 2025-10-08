# general
import time
from pathlib import Path
import threading

# third-party
import requests

# spatial
import xarray as xa

# parallel
import concurrent.futures

# rich
from rich.console import Console

# custom
from esgpull.esgpullplus import ui


class DownloadSubset:
    def __init__(
        self,
        files,
        fs,
        output_dir=None,
        subset=None,
        max_workers=4,
        force_direct_download=False,
    ):
        self.files = files
        self.fs = fs
        self.output_dir = output_dir
        self.subset = subset
        self.max_workers = max_workers
        self.force_direct_download = force_direct_download
        self._shutdown_requested = threading.Event()

    def get_file_path(self, file):
        if self.output_dir:
            return Path(self.output_dir) / file.filename
        else:
            return self.fs[file].drs

    def file_exists(self, file):
        file_path = self.get_file_path(file)
        exists = file_path.exists() and file_path.stat().st_size > 0
        if not exists:
            # Clean up any leftover .part files
            part_file = file_path.with_suffix(file_path.suffix + ".part")
            if part_file.exists():
                try:
                    part_file.unlink()
                except Exception:
                    pass
        return exists

    def run(self):
        """Simple download workflow without complex threading."""
        file_str = "files" if len(self.files) > 1 else "file"
        console = Console()
        console.print(f"Attempting download of {len(self.files)} {file_str}...")

        start_time = time.localtime()
        console.print(
            f":clock3: START: {time.strftime('%Y-%m-%d %H:%M:%S', start_time)}\n"
        )

        # Filter out existing files
        files_to_download = [f for f in self.files if not self.file_exists(f)]

        if not files_to_download:
            console.print("All files already exist, nothing to download.")
            return

        console.print(f"Downloading {len(files_to_download)} new files...")

        # Simple UI without complex threading
        with ui.DownloadProgressUI(files_to_download) as ui_instance:
            # Process files in parallel
            with concurrent.futures.ThreadPoolExecutor(
                max_workers=self.max_workers
            ) as executor:
                futures = {
                    executor.submit(self._process_file_simple, file, ui_instance): file
                    for file in files_to_download
                }

                # Wait for completion
                for future in concurrent.futures.as_completed(futures):
                    if self._shutdown_requested.is_set():
                        break

        end_time = time.localtime()
        console.print(f"\n:clock3: END: {time.strftime('%Y-%m-%d %H:%M:%S', end_time)}")

    def _process_file_simple(self, file, ui_instance):
        """Simple file processing without complex error handling."""
        try:
            ui_instance.set_status(file, "STARTING", "cyan")

            # Check if we should use direct download
            if self._is_direct_download_needed(file) or self.force_direct_download:
                success = self._download_file_direct(file, ui_instance)
            else:
                success = self._download_via_xarray(file, ui_instance)

            if success:
                ui_instance.set_status(file, "DONE", "green")
                # Hide completed files after 3 seconds to declutter
                self._hide_file_after_delay(file, ui_instance, 3)
            else:
                ui_instance.set_status(file, "FAILED", "red")

        except Exception as e:
            ui_instance.set_status(file, "ERROR", "red")
            ui_instance.add_failed(file, f"Error: {e}", e)

    def _is_direct_download_needed(self, file):
        """Check if direct download should be used."""
        if self.force_direct_download:
            return True
        else:
            return False

    def _download_via_xarray(self, file, ui_instance):
        """Download file via xarray (load -> save)."""
        try:
            ui_instance.set_status(file, "OPENING", "cyan")

            # Try to open with xarray
            ds = self._open_dataset_simple(file)
            if ds is None:
                return False

            # Apply subset if needed
            if self.subset:
                subset_dims = {
                    k: v
                    for k, v in self.subset.items()
                    if k in ds.dims or k in ds.coords
                }
                ds = ds.isel(**subset_dims)

            # Load data
            ui_instance.set_status(file, "LOADING", "blue")

            # Simple progress for xarray loading
            if hasattr(ui_instance, "update_file_progress"):
                # Count data variables for progress tracking
                data_vars = list(ds.data_vars.keys())
                total_vars = len(data_vars)

                # Load variables one by one with progress updates
                for i, var_name in enumerate(data_vars):
                    if hasattr(ds[var_name].data, "compute"):
                        ds[var_name].load()  # Load this variable
                    ui_instance.update_file_progress(file, i + 1, total_vars)

            # Ensure all data is loaded
            ds.load()

            # Save file
            ui_instance.set_status(file, "SAVING", "yellow")
            return self._save_dataset(file, ds)

        except Exception as e:
            print(f"xarray download failed for {file.filename}: {e}")
            return False

    def _download_file_direct(self, file, ui_instance):
        """Simple direct HTTP download."""
        file_path = self.get_file_path(file)
        temp_path = file_path.with_suffix(file_path.suffix + ".part")

        try:
            ui_instance.set_status(file, "DOWNLOADING", "blue")

            # Check for existing download
            if temp_path.exists():
                time.sleep(2)  # Wait briefly for other instance
                if file_path.exists() and file_path.stat().st_size > 0:
                    return True  # Another instance completed it

            # Download
            response = requests.get(file.url, stream=True, timeout=(30, 300))
            response.raise_for_status()

            file_path.parent.mkdir(parents=True, exist_ok=True)
            if temp_path.exists():
                temp_path.unlink()

            with open(temp_path, "wb") as f:
                downloaded = 0
                total_size = int(response.headers.get("content-length", 0))

                for chunk in response.iter_content(chunk_size=8192):
                    if chunk and not self._shutdown_requested.is_set():
                        f.write(chunk)
                        downloaded += len(chunk)

                        # Update progress every 64KB to avoid UI spam
                        if downloaded % (64 * 1024) == 0 or total_size > 0:
                            if hasattr(ui_instance, "update_file_progress"):
                                ui_instance.update_file_progress(
                                    file, downloaded, total_size
                                )

            # Atomic rename
            if temp_path.exists() and temp_path.stat().st_size > 0:
                temp_path.rename(file_path)
                return True

        except Exception as e:
            print(f"Direct download failed for {file.filename}: {e}")
            # Cleanup
            for path in [temp_path, file_path]:
                if path.exists():
                    try:
                        path.unlink()
                    except Exception:
                        pass

        return False

    def _open_dataset_simple(self, file):
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

    def _save_dataset(self, file, ds):
        """Simple dataset saving with attribute cleaning."""
        file_path = self.get_file_path(file)
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
            print(f"Save failed for {file.filename}: {e}")
            # Cleanup
            for path in [temp_path, file_path]:
                if path.exists():
                    try:
                        path.unlink()
                    except Exception:
                        pass

        return False

    def _clean_attributes(self, ds):
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

    def _hide_file_after_delay(self, file, ui_instance, delay_seconds):
        """Hide a completed file from the UI after a delay to declutter."""

        def hide_file():
            time.sleep(delay_seconds)
            try:
                # Check if the UI instance has a method to hide files
                if hasattr(ui_instance, "hide_file"):
                    ui_instance.hide_file(file)
                elif hasattr(ui_instance, "file_task_ids") and hasattr(
                    ui_instance, "progress"
                ):
                    # Fallback: try to hide the progress bar directly
                    task_id = ui_instance.file_task_ids.get(file.file_id)
                    if task_id is not None:
                        ui_instance.progress.update(task_id, visible=False)
            except Exception:
                pass  # Silently ignore if hiding fails

        # Run the hide operation in a separate thread so it doesn't block
        hide_thread = threading.Thread(target=hide_file, daemon=True)
        hide_thread.start()
