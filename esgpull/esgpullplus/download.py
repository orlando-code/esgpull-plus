# general
import threading
import time
from pathlib import Path

# third-party
import requests

# spatial
import xarray as xa

# from esgpulllite.custom import ui

from esgpull.esgpullplus import ui
from esgpull import utils
# parallel
import concurrent.futures


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
        self.max_workers = max_workers if max_workers < len(files) else len(files)
        self.force_direct_download = force_direct_download
        self._shutdown_requested = threading.Event()

    def get_file_path(self, file):
        if self.output_dir:
            return Path(self.output_dir) / file.filename
        else:
            # return self.fs[file].drs
            return self.fs.paths.data / file.local_path / file.filename

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
        from rich.console import Console

        console = Console()
        start_time = time.localtime()
        console.print(
            f":clock3: START: {time.strftime('%Y-%m-%d %H:%M:%S', start_time)}\n"
        )

        files_to_download = [f for f in self.files if not self.file_exists(f)]
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
                for future in concurrent.futures.as_completed(futures):
                    if self._shutdown_requested.is_set():
                        break
            ui_instance.print_summary()
        end_time = time.localtime()
        console.print(f"\n:clock3: END: {time.strftime('%Y-%m-%d %H:%M:%S', end_time)}")

    def _process_file_with_ui(self, file, ui_instance):
        try:
            ui_instance.set_status(file, "STARTING", "cyan")
            success = self._download_file_direct_ui(file, ui_instance)
            if success:
                ui_instance.set_status(file, "DONE", "green")
                ui_instance.complete_file(file)
                # Hide the file's progress bar after 5 seconds
                self.hide_file_after_delay(file, ui_instance, 5)
            else:
                ui_instance.set_status(file, "FAILED", "red")
                ui_instance.complete_file(file)
        except Exception as e:
            ui_instance.set_status(file, "ERROR", "red")
            ui_instance.add_failed(file, f"Error: {e}", e)
            ui_instance.complete_file(file)

    def _is_direct_download_needed(self, file):
        """Check if direct download should be used."""
        if self.force_direct_download:
            return True
        else:
            return False

    def _download_via_xarray_ui(self, file, ui_instance):
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
            print(f"xarray download failed for {file.filename}: {e}")
            return False

    def _download_file_direct_ui(self, file, ui_instance):
        file_path = self.get_file_path(file)
        temp_path = file_path.with_suffix(file_path.suffix + ".part")
        try:
            ui_instance.set_status(file, "DOWNLOADING", "blue")
            if temp_path.exists():
                time.sleep(2)
                if file_path.exists() and file_path.stat().st_size > 0:
                    return True
            response = requests.get(file.url, stream=True, timeout=(30, 300))
            response.raise_for_status()
            file_path.parent.mkdir(parents=True, exist_ok=True)
            if temp_path.exists():
                temp_path.unlink()
            total = int(response.headers.get("content-length", file.size or 0))
            bytes_downloaded = 0
            # Set the per-file progress bar total to the file size (if available)
            ui_instance.update_file_progress(file, 0, total)
            with open(temp_path, "wb") as f:
                for chunk in response.iter_content(chunk_size=8192):
                    if chunk and not self._shutdown_requested.is_set():
                        f.write(chunk)
                        bytes_downloaded += len(chunk)
                        ui_instance.update_file_progress(file, bytes_downloaded, total)
            if temp_path.exists() and temp_path.stat().st_size > 0:
                temp_path.rename(file_path)
                return True
        except Exception as e:
            print(f"Direct download failed for {file.filename}: {e}")
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

    # Add a method to hide file after delay, called from main thread
    def hide_file_after_delay(self, file, ui_instance, delay_seconds):
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
