import asyncio
from pathlib import Path

import pytest

from esgpull.esgpullplus import file_watcher as fw


def test_async_regrid_processor_scan_existing_files_and_queue(tmp_path):
    """
    scan_existing_files should queue .nc files under watch_dir, excluding
    already regridded outputs and weight/cache directories.
    """
    watch_dir = tmp_path / "watch"
    watch_dir.mkdir()

    # Ordinary NetCDF file that should be queued
    keep = watch_dir / "tas_2000.nc"
    keep.write_bytes(b"dummy")

    # Already regridded output in reprojected/ should be skipped
    reproj_dir = watch_dir / "reprojected"
    reproj_dir.mkdir()
    (reproj_dir / "tas_2000.nc").write_bytes(b"dummy")

    # File inside cdo_weights should be skipped
    weights_dir = watch_dir / "cdo_weights"
    weights_dir.mkdir()
    (weights_dir / "weights.nc").write_bytes(b"dummy")

    proc = fw.AsyncRegridProcessor(
        watch_dir=watch_dir,
        batch_size=2,
        batch_timeout=0.1,
        validate_can_open=False,  # dummy .nc not openable; test only queue vs skip logic
    )

    async def run():
        await proc.scan_existing_files()
        queued = []
        while not proc.file_queue.empty():
            queued.append(await proc.file_queue.get())
        return queued

    queued = asyncio.run(run())
    # Only the plain tas_2000.nc should be queued
    assert queued == [keep]


def test_async_regrid_processor_process_batch_uses_stubbed_regrid(tmp_path, monkeypatch):
    """
    _process_batch should call _process_single_file_standalone for each
    queued file and respect the 'success' / 'skipped' flags from results.
    We stub out the heavy CDO work and use ThreadPoolExecutor so the test
    does not spawn processes (avoids PermissionError in CI/sandbox).
    """
    from concurrent.futures import ThreadPoolExecutor

    watch_dir = tmp_path / "watch"
    watch_dir.mkdir()
    f1 = watch_dir / "a.nc"
    f2 = watch_dir / "b.nc"
    f1.write_bytes(b"dummy")
    f2.write_bytes(b"dummy")

    proc = fw.AsyncRegridProcessor(
        watch_dir=watch_dir,
        batch_size=10,
        batch_timeout=0.1,
    )

    calls = []

    def fake_process_single_file_standalone(
        file_path,
        output_dir,
        target_resolution,
        target_grid,
        weight_cache_dir,
        extract_surface,
        extract_seafloor,
        use_regrid_cache,
        use_seafloor_cache,
        max_memory_gb,
        chunk_size_gb,
        enable_chunking,
        overwrite=False,
        representative_file=None,
        verbose=False,
        error_log_path=None,
    ):
        calls.append((file_path, output_dir))
        return {
            "success": True,
            "file_path": file_path,
            "skipped": False,
            "message": "ok",
            "stats": {},
        }

    monkeypatch.setattr(
        "esgpull.esgpullplus.cdo_regrid._process_single_file_standalone",
        fake_process_single_file_standalone,
        raising=True,
    )
    # Use ThreadPoolExecutor so we don't spawn processes (avoids PermissionError in CI)
    monkeypatch.setattr(
        "concurrent.futures.ProcessPoolExecutor",
        ThreadPoolExecutor,
    )

    async def run():
        await proc._process_batch([f1, f2])

    asyncio.run(run())
    assert {c[0] for c in calls} == {f1, f2}

