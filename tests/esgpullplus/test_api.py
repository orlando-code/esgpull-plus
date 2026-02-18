import types
from pathlib import Path

import pytest

from esgpull.esgpullplus import api as api_mod


class DummyAPI(api_mod.EsgpullAPI):
    """
    Lightweight EsgpullAPI subclass that lets us monkeypatch networked
    behaviour while still exercising esgpullplus-specific logic.
    """

    def __init__(self):
        # Bypass heavy Esgpull construction; tests will patch methods that use it
        pass  # type: ignore[call-arg]


def test_find_alternative_files_prefers_other_data_nodes(monkeypatch):
    """find_alternative_files should pick files on a different data_node first."""

    api = DummyAPI()

    failed_file = {
        "file_id": "orig-file",
        "dataset_id": "ds1",
        "variable": "tas",
        "experiment_id": "historical",
        "frequency": "mon",
        "data_node": "esgf.ichec.ie",
        "version": "v20200101",
        "nominal_resolution": "100 km",
        "filename": "tas_Amon_model_historical_r1i1p1f1_gn_200001-200012.nc",
    }

    # Two candidates: one on same node, one on a different node,
    # otherwise identical. The different-node file should be ranked first.
    candidates = [
        {
            "file_id": "alt-same-node",
            "dataset_id": "ds2",
            "variable": "tas",
            "experiment_id": "historical",
            "frequency": "mon",
            "data_node": "esgf.ichec.ie",
            "version": "v20200101",
            "nominal_resolution": "100 km",
            "filename": "tas_Amon_model_historical_r2i1p1f1_gn_200001-200012.nc",
        },
        {
            "file_id": "alt-other-node",
            "dataset_id": "ds3",
            "variable": "tas",
            "experiment_id": "historical",
            "frequency": "mon",
            "data_node": "esgf-data.dkrz.de",
            "version": "v20200101",
            "nominal_resolution": "100 km",
            "filename": "tas_Amon_model_historical_r3i1p1f1_gn_200001-200012.nc",
        },
    ]

    def fake_search(criteria):
        # Sanity-check that expected facets are used
        assert criteria["variable"] == "tas"
        assert criteria["experiment_id"] == "historical"
        assert criteria["frequency"] == "mon"
        return candidates

    monkeypatch.setattr(api, "search", fake_search)

    alts = api.find_alternative_files(failed_file, exclude_file_ids=["orig-file"])

    # We should get both candidates back, ordered by score so that the
    # different data_node entry comes first.
    assert [a["file_id"] for a in alts] == ["alt-other-node", "alt-same-node"]


def test_find_alternative_files_filters_original_dataset(monkeypatch):
    """Original dataset/file ids must be excluded from alternatives."""

    api = DummyAPI()
    failed_file = {
        "file_id": "file-1",
        "dataset_id": "ds1",
        "variable": "tas",
        "experiment_id": "historical",
        "frequency": "mon",
        "data_node": "esgf.ichec.ie",
    }

    def fake_search(criteria):
        return [
            {
                "file_id": "file-1",  # same file_id, must be excluded
                "dataset_id": "ds1",  # same dataset_id, must be excluded
                "variable": "tas",
                "experiment_id": "historical",
                "frequency": "mon",
                "data_node": "esgf-data.dkrz.de",
            }
        ]

    monkeypatch.setattr(api, "search", fake_search)

    alts = api.find_alternative_files(failed_file, exclude_file_ids=["file-1"])
    assert alts == []  # nothing usable remains


def test_search_and_download_batches_and_wires_downloadsubset(monkeypatch, tmp_path):
    """
    search_and_download should:
      - call SearchResults.run() once
      - batch files according to batch_size / adaptive batch size
      - instantiate DownloadSubset with find_alternatives & api_instance
      - call DownloadSubset.run() for each batch
    """

    # Create a small EnhancedFile-like stub for the test
    class DummyFile:
        def __init__(self, fid: str):
            self.file_id = fid
            self.filename = f"{fid}.nc"
            self.local_path = Path("dummy")
            self.size = 123

    files = [DummyFile(f"file-{i}") for i in range(7)]

    # Stub SearchResults to avoid hitting real ESGF or filesystem
    class DummySearchResults:
        def __init__(self, *args, **kwargs):
            self._called_with = (args, kwargs)

        def check_system_resources(self, output_dir):
            assert output_dir  # sanity-check the wiring

        def _get_adaptive_batch_size(self, requested_batch_size, total_files):
            # Keep behaviour deterministic for the test
            assert total_files == len(files)
            return requested_batch_size

        def run(self):
            return files

    monkeypatch.setattr(
        api_mod.search, "SearchResults", DummySearchResults, raising=True
    )

    # Capture constructed DownloadSubset instances instead of performing real downloads
    constructed_subsets = []

    class DummyFS:
        class Paths:
            data = tmp_path

        paths = Paths()

    class DummyDownloadSubset:
        def __init__(self, *args, **kwargs):
            constructed_subsets.append(kwargs)

        def run(self):
            # No-op for tests
            return

    monkeypatch.setattr(
        api_mod.download, "DownloadSubset", DummyDownloadSubset, raising=True
    )

    # Use a lightweight API object with a stub fs
    fake_api = types.SimpleNamespace(esg=types.SimpleNamespace(fs=DummyFS()))

    search_criteria = {"project": "CMIP6", "variable": "tas"}
    meta_criteria = {
        "data_dir": str(tmp_path),
        "batch_size": 3,
        "test": False,
        "find_alternatives": True,
        "max_workers": 4,
    }

    api_mod.search_and_download(
        search_criteria=search_criteria,
        meta_criteria=meta_criteria,
        API=fake_api,
        symmetrical=False,
        symmetrical_sources_cache=None,
    )

    # 7 files with batch size 3 -> 3 batches
    assert len(constructed_subsets) == 3

    # Every constructed subset should have find_alternatives=True and api_instance set
    for kwargs in constructed_subsets:
        assert kwargs["find_alternatives"] is True
        assert kwargs["api_instance"] is fake_api

