from pathlib import Path

import pytest

from esgpull.esgpullplus import download as dl_mod


class DummyFS:
    class Paths:
        def __init__(self, data):
            self.data = data

    def __init__(self, data_dir: Path):
        self.paths = self.Paths(data_dir)


class DummyFile:
    """Minimal EnhancedFile-like stub for DownloadSubset tests."""

    def __init__(
        self,
        file_id: str,
        local_root: Path,
        *,
        filename: str | None = None,
        local_path: str | Path = "subdir",
    ):
        self.file_id = file_id
        self.filename = filename or f"{file_id}.nc"
        self.local_path = Path(local_path) if not isinstance(local_path, Path) else local_path
        self.size = 10
        self.url = "http://example.com/file.nc"
        self._root = local_root


def test_get_file_path_uses_output_dir(tmp_path):
    fs = DummyFS(tmp_path / "data")
    file = DummyFile("f1", tmp_path)

    subset = dl_mod.DownloadSubset(files=[file], fs=fs, output_dir=tmp_path / "out")

    path = subset._get_file_path(file)
    assert path == tmp_path / "out" / file.filename


def test_get_file_path_uses_data_dir_when_no_output_dir(tmp_path):
    fs = DummyFS(tmp_path / "data")
    file = DummyFile("f1", tmp_path)

    subset = dl_mod.DownloadSubset(files=[file], fs=fs, data_dir=tmp_path / "custom")

    path = subset._get_file_path(file)
    assert path == tmp_path / "custom" / file.local_path / file.filename


def test_find_existing_path_ignores_version_mismatch(tmp_path):
    fs = DummyFS(tmp_path / "data")
    local = Path(
        "CMIP6/CMIP/EC-Earth-Consortium/EC-Earth3/historical/r3i1p1f1/Omon/tos/gn/v20200514"
    )
    filename = "tos_Omon_EC-Earth3_historical_r3i1p1f1_gn_195601-195612.nc"
    on_disk = (
        tmp_path
        / "CMIP6/CMIP/EC-Earth-Consortium/EC-Earth3/historical/r3i1p1f1/Omon/tos/gn/v20200730"
    )
    on_disk.mkdir(parents=True)
    (on_disk / filename).write_bytes(b"x" * 128)

    file = DummyFile("f1", tmp_path, filename=filename, local_path=local)
    subset = dl_mod.DownloadSubset(files=[file], fs=fs, data_dir=tmp_path)

    found = subset._find_existing_path(file)
    assert found == on_disk / filename
    assert subset._file_exists(file) is True


def test_variant_coverage_accepts_ec_earth3_hr(tmp_path):
    fs = DummyFS(tmp_path / "data")
    local = Path(
        "CMIP6/CMIP/EC-Earth-Consortium/EC-Earth3/historical/r3i1p1f1/Omon/tos/gn/v20200514"
    )
    requested = "tos_Omon_EC-Earth3_historical_r3i1p1f1_gn_195601-195612.nc"
    hr_path = (
        tmp_path
        / "CMIP6/CMIP/EC-Earth-Consortium/EC-Earth3-HR/historical/r1i1p1f1/Omon/tos/gn/v20250225"
    )
    hr_path.mkdir(parents=True)
    hr_file = hr_path / "tos_Omon_EC-Earth3-HR_historical_r1i1p1f1_gn_195601-195612.nc"
    hr_file.write_bytes(b"x" * 128)

    file = DummyFile("f1", tmp_path, filename=requested, local_path=local)
    subset = dl_mod.DownloadSubset(files=[file], fs=fs, data_dir=tmp_path)

    assert subset._variant_coverage_exists(file) == hr_file
    assert subset._file_exists(file) is True


def test_try_alternative_file_invokes_api_and_download(monkeypatch, tmp_path):
    """
    _try_alternative_file should:
      - call api_instance.find_alternative_files
      - attempt to download each returned alternative
      - return True on first successful alternative
    """

    fs = DummyFS(tmp_path / "data")
    file = DummyFile("f1", tmp_path)

    alt_file_dict = {
        "file_id": "alt-1",
        "dataset_id": "ds2",
        "variable": "tas",
        "experiment_id": "historical",
        "frequency": "mon",
        "data_node": "alt.node",
        "filename": "alt-1.nc",
        "local_path": "subdir",
        "size": 10,
    }

    class DummyAPI:
        def __init__(self):
            self.called_with = None

        def find_alternative_files(self, failed_file_dict, exclude_file_ids=None):
            self.called_with = (failed_file_dict, exclude_file_ids)
            return [alt_file_dict]

    api_instance = DummyAPI()

    # Capture which files the UI sees for status/progress updates
    status_updates = []

    class DummyUI:
        def set_status(self, file_obj, status, color):
            status_updates.append((file_obj.filename, status))

        def complete_file(self, file_obj):
            status_updates.append((file_obj.filename, "COMPLETE"))

        def update_file_progress(self, file_obj, done, total):
            # not relevant for this test
            pass

        def add_failed(self, *args, **kwargs):
            pass

    ui = DummyUI()

    subset = dl_mod.DownloadSubset(
        files=[file],
        fs=fs,
        output_dir=tmp_path / "out",
        api_instance=api_instance,
        find_alternatives=True,
    )

    # Avoid real network I/O; mark download as failing for original file
    # and succeeding for the alternative file
    def fake_download(original_file, ui_instance, max_retries=3):
        if original_file.filename == file.filename:
            return False
        return True

    monkeypatch.setattr(subset, "_download_file_direct_ui", fake_download)

    ok = subset._try_alternative_file(file, ui_instance=ui, original_error=None)

    assert ok is True
    # Ensure api was queried with the failed file and its file_id excluded
    failed_dict, excluded_ids = api_instance.called_with
    assert failed_dict["file_id"] == file.file_id
    assert excluded_ids == [file.file_id]
