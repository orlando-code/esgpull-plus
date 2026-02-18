"""
Enhanced context wrapper that returns EnhancedFile objects for ESGF search results.

Thread-safe implementation that temporarily patches ResultFiles.process during
file searches to produce EnhancedFile objects instead of base File objects.
Also supports enhanced dataset search with full metadata (variable_id, institution_id,
source_id, experiment_id, etc.) for source availability analysis.
"""

import logging
import threading
from typing import Any, Sequence

from esgpull.config import Config
from esgpull.context import (
    Context as OriginalContext,
    ResultDatasets,
    ResultFiles,
)
from esgpull.esgpullplus.enhanced_file import EnhancedFile
from esgpull.models import Query
from esgpull.models.utils import find_int, find_str
from esgpull.tui import logger

# Extended fields for dataset search - needed for source analysis
DATASET_FIELDS_EXTENDED = ["*"]

_patch_lock = threading.Lock()

log = logging.getLogger(__name__)


class _EnhancedDatasetRecord:
    """Wrapper for dataset dict so context._datasets can use .dataset_id."""
    __slots__ = ("_data", "dataset_id")

    def __init__(self, data: dict):
        self._data = data
        self.dataset_id = data["dataset_id"]

    def asdict(self) -> dict:
        return self._data


def _dataset_doc_to_dict(doc: dict) -> _EnhancedDatasetRecord:
    """
    Convert Solr Dataset document to dict with full metadata for search_analysis.
    Uses variable_id as 'variable' for compatibility with analysis code.
    """
    instance_id = find_str(doc.get("instance_id", "")).partition("|")[0]
    if "." in instance_id:
        master_id, version = instance_id.rsplit(".", 1)
    else:
        master_id, version = instance_id, ""
    row = {
        "dataset_id": instance_id,
        "master_id": master_id,
        "version": version,
        "data_node": find_str(doc.get("data_node", "")),
        "size": find_int(doc.get("size", 0)),
        "number_of_files": find_int(doc.get("number_of_files", 0)),
        "variable": doc.get("variable_id")[0] or doc.get("variable")[0],
        "variable_id": doc.get("variable_id")[0],
        "institution_id": doc.get("institution_id")[0],
        "source_id": doc.get("source_id")[0],
        "experiment_id": doc.get("experiment_id")[0],
        "member_id": doc.get("member_id")[0] if doc.get("member_id") else "",
        "nominal_resolution": doc.get("nominal_resolution")[0] if doc.get("nominal_resolution") else "",
        "table_id": doc.get("table_id")[0] if doc.get("table_id") else "",
        "frequency": doc.get("frequency")[0] if doc.get("frequency") else "",
        "project": doc.get("project")[0] if doc.get("project") else "",
    }   # N.B. jankily subsetting the first element of the list for each field – have only seen these return one value per list but may cause issues in future
    return _EnhancedDatasetRecord(row)


def _enhanced_process_files(result_self):
    """Process method that creates EnhancedFile objects instead of File objects."""
    result_self.data = []
    if result_self.success:
        for doc in result_self.json["response"]["docs"]:
            try:
                file = EnhancedFile.serialize(doc)
                if not file.url or not file.url.startswith("http"):
                    log.warning(
                        "Skipping file with empty/invalid URL: %s (data_node=%s)",
                        getattr(file, "filename", "?"),
                        getattr(file, "data_node", "?"),
                    )
                    continue
                result_self.data.append(file)
            except KeyError as exc:
                logger.exception(exc)
                fid = doc.get("instance_id", "unknown")
                logger.warning(f"File {fid} has invalid metadata")
    result_self.processed = True


def _enhanced_process_datasets(result_self):
    """Process method that produces dicts with full metadata from Dataset search."""
    result_self.data = []
    if result_self.success:
        for doc in result_self.json["response"]["docs"]:
            try:
                result_self.data.append(_dataset_doc_to_dict(doc))
            except (KeyError, ValueError) as exc:
                logger.exception(exc)
                did = doc.get("instance_id", "unknown")
                logger.warning(f"Dataset {did} has invalid metadata")
    result_self.processed = True


class EnhancedContext:
    """
    Enhanced context wrapper that returns EnhancedFile objects for file searches.

    Uses a thread-safe lock when patching ResultFiles.process so that concurrent
    searches from different threads don't interfere with each other.

    Args:
        config: esgpull Config instance. If None, uses Config.default().
        noraise: If True, log exceptions instead of raising them during fetch.
    """

    def __init__(self, config: Config | None = None, noraise: bool = False):
        if config is not None:
            self._original_context = OriginalContext(config, noraise=noraise)
        else:
            self._original_context = OriginalContext(noraise=noraise)

    def search(
        self,
        *queries: Query,
        file: bool,
        hits: list[int] | None = None,
        offset: int = 0,
        max_hits: int | None = 200,
        page_limit: int | None = None,
        date_from: Any = None,
        date_to: Any = None,
        keep_duplicates: bool = True,
    ) -> Sequence[EnhancedFile | dict[str, Any]]:
        """
        Search ESGF, returning EnhancedFile objects for file searches or
        dicts with full metadata for dataset searches (file=False).

        For dataset search (file=False), uses extended fields and returns
        dicts with variable, institution_id, source_id, experiment_id, etc.
        for source analysis - much faster than file search (fewer records).
        """
        # Early return for known-empty searches
        if hits is not None and (not hits or sum(hits) == 0):
            log.warning("Search has empty or zero-sum hits, returning empty results")
            return []

        kwargs = dict(
            file=file,
            hits=hits,
            offset=offset,
            max_hits=max_hits,
            page_limit=page_limit,
            date_from=date_from,
            date_to=date_to,
            keep_duplicates=keep_duplicates,
        )

        if file:
            # File search: patch ResultFiles to produce EnhancedFile
            with _patch_lock:
                original_process = ResultFiles.process
                ResultFiles.process = _enhanced_process_files
                try:
                    return self._original_context.search(*queries, **kwargs)
                except (IndexError, ValueError) as e:
                    if "index" in str(e).lower():
                        log.warning(f"Search returned empty/invalid hits: {e}")
                        return []
                    raise
                finally:
                    ResultFiles.process = original_process
        else:
            # Dataset search: use extended fields, patch ResultDatasets for full metadata
            with _patch_lock:
                original_process = ResultDatasets.process
                ResultDatasets.process = _enhanced_process_datasets
                try:
                    if hits is None:
                        hits = self._original_context.hits(*queries, file=False)
                    prep_results = self._original_context.prepare_search(
                        *queries,
                        file=False,
                        hits=hits,
                        offset=offset,
                        max_hits=max_hits,
                        page_limit=page_limit,
                        date_from=date_from,
                        date_to=date_to,
                        fields_param=DATASET_FIELDS_EXTENDED,
                    )
                    coro = self._original_context._datasets(
                        *prep_results,
                        keep_duplicates=keep_duplicates,
                    )
                    return self._original_context._sync(coro)
                except (IndexError, ValueError) as e:
                    if "index" in str(e).lower():
                        log.warning(f"Search returned empty/invalid hits: {e}")
                        return []
                    raise
                finally:
                    ResultDatasets.process = original_process
