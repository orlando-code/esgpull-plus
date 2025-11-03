import signal
import sys
import threading
import time
from pathlib import Path
from typing import Any, Dict, List, Optional, cast
from datetime import datetime, timezone

from rich import print as rich_print


from esgpull.cli.utils import (
    parse_query,
    serialize_queries_from_file,
)
# TO DO
# XX processing box

# custom
from esgpull.esgpullplus import api, download, fileops, search, esgpuller
from esgpull.esgpullplus.enhanced_context import Context
from esgpull.graph import Graph
from esgpull.models import File, Query
from esgpull.tui import Verbosity
from esgpull.utils import sync

# Global flag for graceful shutdown
_shutdown_requested = threading.Event()


class GracefulInterruptHandler:
    """Handle interruption signals gracefully."""

    def __init__(self):
        self.interrupted = False
        self.original_sigint = signal.signal(signal.SIGINT, self._signal_handler)
        self.original_sigterm = signal.signal(signal.SIGTERM, self._signal_handler)

    def _signal_handler(self, signum, frame):
        """Handle interrupt signals."""
        signal_name = "SIGINT" if signum == signal.SIGINT else "SIGTERM"
        if not self.interrupted:
            rich_print(
                f"\n[bold yellow]⚠️  {signal_name} received. Initiating graceful shutdown...[/bold yellow]"
            )
            rich_print("[yellow]• Stopping new downloads[/yellow]")
            rich_print("[yellow]• Cleaning up active operations[/yellow]")
            rich_print("[yellow]• Press Ctrl+C again to force quit[/yellow]")
            self.interrupted = True
            _shutdown_requested.set()
            # Brief pause to show shutdown messages
            time.sleep(0.5)
        else:
            rich_print(
                "\n[bold red]💀 Force quit requested. Exiting immediately.[/bold red]"
            )
            sys.exit(1)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        # Restore original handlers
        signal.signal(signal.SIGINT, self.original_sigint)
        signal.signal(signal.SIGTERM, self.original_sigterm)


class EsgpullAPI:
    """
    Python API for interacting with esgpull, using the same logic as the CLI scripts.
    """
    # TODO: look over distributed search logic
    def __init__(
        self,
        config_path: Optional[str | Path] = None,
        verbosity: str = "detail",
    ):
        self.verbosity = Verbosity[verbosity.capitalize()]
        self.esg = esgpuller.Esgpull(path=config_path, verbosity=self.verbosity)

    def search(self, criteria: Dict[str, Any]) -> List[Dict[str, Any]]:
        """
        Searches ESGF nodes for files/datasets matching the criteria.
        Uses distributed search to query multiple nodes, with retry logic for 500 errors.

        Args:
            criteria (dict): A dictionary of search facets (e.g., project, variable).
                      Can include 'limit' to restrict the number of results.

        Returns:
            A list of dictionaries, where each dictionary represents a found file/dataset.
        """
        # Use the same logic as CLI: parse_query, then context.search
        _criteria = criteria.copy()
        max_hits = _criteria.pop("limit", None)
        tags = _criteria.pop("tags", [])
        # Remove filter key as it's not a search facet
        _criteria.pop("filter", None)
        
        # Try distributed search first (queries multiple nodes)
        query = parse_query(
            facets=[f"{k}:{v}" for k, v in _criteria.items()],
            tags=tags,
            require=None,
            distrib="true",  # Enable distributed search to query multiple nodes
            latest=None,
            replica=None,
            retracted=None,
        )
        query.compute_sha()
        self.esg.graph.resolve_require(query)
        
        try:
            # Use a fresh EnhancedContext bound to a fresh OriginalContext to avoid event-loop/thread issues
            from esgpull.esgpullplus.enhanced_context import EnhancedContext
            from esgpull.context import Context as OriginalContext
            local_original = OriginalContext(self.esg.config, noraise=True)
            local_ctx = EnhancedContext()
            local_ctx._original_context = local_original
            results = local_ctx.search(query, file=True, max_hits=max_hits)
            return [result.asdict() for result in results]
        except Exception as e:
            # If distributed search fails with 500 error, try alternative nodes
            if self._is_server_error(e):
                rich_print(
                    "[yellow]⚠️  Distributed search failed with server error. "
                    "Retrying with alternative ESGF nodes...[/yellow]"
                )
                return self._search_with_retry(_criteria, tags, max_hits)
            raise
    
    def _is_server_error(self, exception: Exception) -> bool:
        """Check if exception is a server error (500, 502, 503, etc.)."""
        import httpx
        if isinstance(exception, httpx.HTTPStatusError):
            return exception.response.status_code >= 500
        if isinstance(exception, BaseExceptionGroup):
            return any(
                isinstance(exc, httpx.HTTPStatusError) and exc.response.status_code >= 500
                for exc in exception.exceptions
            )
        return False
    
    def _search_with_retry(
        self,
        criteria: Dict[str, Any],
        tags: list[str],
        max_hits: Optional[int],
    ) -> List[Dict[str, Any]]:
        """
        Retry search with alternative ESGF nodes when primary search fails.
        
        Args:
            criteria: Search criteria
            tags: Query tags
            max_hits: Maximum number of results
            
        Returns:
            List of search results
        """
        # List of alternative ESGF nodes to try
        alternative_nodes = [
            "esgf-data.dkrz.de",
            "esgf.ceda.ac.uk",
            "esgf-node.llnl.gov",
            "esgf-node.ornl.gov",
        ]
        
        # Try to fetch available nodes first
        try:
            available_nodes = self.esg.fetch_index_nodes()
            # Filter to known reliable nodes
            known_nodes = [
                node for node in available_nodes
                if any(alt in node for alt in alternative_nodes + ["ipsl", "dkrz", "ceda", "llnl", "ornl"])
            ]
            if known_nodes:
                alternative_nodes = known_nodes[:5]  # Limit to top 5 nodes
        except Exception:
            # If we can't fetch nodes, use hardcoded list
            pass
        
        query = parse_query(
            facets=[f"{k}:{v}" for k, v in criteria.items()],
            tags=tags,
            require=None,
            distrib="false",  # Try single node searches
            latest=None,
            replica=None,
            retracted=None,
        )
        query.compute_sha()
        self.esg.graph.resolve_require(query)
        
        # Try each alternative node (use fresh contexts per attempt to avoid event-loop/thread issues)
        original_node = self.esg.config.api.index_node
        results = None
        
        for node in alternative_nodes:
            try:
                rich_print(f"[blue]🔍 Trying ESGF node: {node}[/blue]")
                # Build a fresh OriginalContext bound to this node (without mutating shared self.esg.context)
                from esgpull.context import Context as OriginalContext
                from esgpull.esgpullplus.enhanced_context import EnhancedContext
                # Temporarily set node for creating the local context
                self.esg.config.api.index_node = node
                local_original = OriginalContext(self.esg.config, noraise=True)
                local_ctx = EnhancedContext()
                local_ctx._original_context = local_original
                results = local_ctx.search(query, file=True, max_hits=max_hits)
                rich_print(f"[green]✅ Successfully searched using node: {node}[/green]")
                # Restore original node
                self.esg.config.api.index_node = original_node
                return [result.asdict() for result in results]
                    
            except Exception as node_error:
                # Restore node before evaluating error
                try:
                    self.esg.config.api.index_node = original_node
                except Exception:
                    pass
                
                if self._is_server_error(node_error):
                    rich_print(
                        f"[yellow]⚠️  Node {node} also returned server error, trying next node...[/yellow]"
                    )
                    continue
                else:
                    # For non-server errors, still try to return results if available
                    if results is not None:
                        return [result.asdict() for result in results]
                    # If no results, continue to next node
                    rich_print(
                        f"[yellow]⚠️  Node {node} returned error: {node_error}, trying next node...[/yellow]"
                    )
                    continue
        
        # If all nodes failed, raise the original error
        raise RuntimeError(
            f"All ESGF nodes failed. Tried: {', '.join(alternative_nodes)}. "
            "This may be a temporary ESGF infrastructure issue."
        )
    
    def find_alternative_files(
        self,
        failed_file: Dict[str, Any],
        exclude_file_ids: Optional[list[str]] = None,
    ) -> List[Dict[str, Any]]:
        """
        Find alternative files for a failed download by searching for files with the same
        dataset characteristics but potentially different data nodes or versions.
        
        Args:
            failed_file: Dictionary representation of the failed file
            exclude_file_ids: List of file IDs to exclude from results (e.g., the failed file)
            
        Returns:
            List of alternative file dictionaries, ordered by preference
        """
        if exclude_file_ids is None:
            exclude_file_ids = []
        
        # Extract key identifying information from the failed file
        search_criteria = {
            "project": failed_file.get("project") or failed_file.get("mip_era", "CMIP6"),
            "variable": failed_file.get("variable") or failed_file.get("variable_id"),
            "experiment_id": failed_file.get("experiment_id"),
            "frequency": failed_file.get("frequency") or failed_file.get("time_frequency"),
        }
        
        # Remove None values
        search_criteria = {k: v for k, v in search_criteria.items() if v is not None}
        
        if not search_criteria.get("variable"):
            # Can't search without variable
            return []
        
        rich_print(
            f"[blue]🔍 Searching for alternative files for {failed_file.get('filename', 'unknown file')}...[/blue]"
        )
        
        try:
            # Search for alternatives using a fresh API/context to avoid event-loop/thread issues
            alt_api = EsgpullAPI(verbosity=str(self.verbosity.name).lower()) if hasattr(self, 'verbosity') else EsgpullAPI()
            alternatives = alt_api.search(search_criteria)
            
            # Filter alternatives
            filtered_alternatives = []
            failed_dataset_id = failed_file.get("dataset_id", "")
            failed_instance_id = failed_file.get("instance_id", "")
            failed_file_id = failed_file.get("file_id", "")
            
            for alt in alternatives:
                alt_file_id = alt.get("file_id", "")
                alt_dataset_id = alt.get("dataset_id", "")
                
                # Skip the original file and excluded files
                if (alt_file_id == failed_file_id or 
                    alt_file_id in exclude_file_ids or
                    alt_dataset_id == failed_dataset_id):
                    continue
                
                # Prefer files from different data nodes
                failed_data_node = failed_file.get("data_node", "")
                alt_data_node = alt.get("data_node", "")
                
                # Match key characteristics
                if (alt.get("variable") == failed_file.get("variable") and
                    alt.get("experiment_id") == failed_file.get("experiment_id") and
                    alt.get("frequency") == failed_file.get("frequency")):
                    
                    # Score alternatives: prefer different data nodes, high resolution, then different versions
                    from esgpull.esgpullplus import utils as search_utils
                    
                    score = 0
                    
                    # Strong preference for different data node (highest priority - try different node when one times out)
                    if alt_data_node and alt_data_node != failed_data_node:
                        score += 10000  # Strong preference for different data node
                    
                    # Prefer higher resolution files (maintains original search sorting preference)
                    # Resolution values: smaller = higher resolution (better)
                    failed_res = search_utils.calc_resolution(
                        failed_file.get("nominal_resolution", "") or ""
                    )
                    alt_res = search_utils.calc_resolution(
                        alt.get("nominal_resolution", "") or ""
                    )
                    
                    # Score resolution: prefer same or better (smaller or equal) resolution
                    if alt_res > 0 and failed_res > 0:
                        if alt_res <= failed_res:
                            # Same or better resolution: give bonus inversely proportional to resolution
                            # Smaller resolution (higher quality) gets more points
                            # Max bonus of 100 for very high resolution (very small value)
                            score += max(0, 100 - alt_res * 10)
                        else:
                            # Lower resolution (larger value): give penalty
                            # The worse the resolution, the larger the penalty
                            score += max(0, 50 - (alt_res - failed_res) * 5)
                    elif alt_res > 0 and failed_res >= 9999.0:
                        # Failed file has no resolution info, but alternative does - prefer it
                        score += 50
                    
                    # Small preference for different version (as tie-breaker)
                    if alt.get("version") != failed_file.get("version"):
                        score += 1
                    
                    filtered_alternatives.append((score, alt))
            
            # Sort by score (highest first) and return
            filtered_alternatives.sort(key=lambda x: x[0], reverse=True)
            result = [alt for _, alt in filtered_alternatives[:5]]  # Return top 5
            
            if result:
                rich_print(
                    f"[green]✅ Found {len(result)} alternative file(s) for {failed_file.get('filename', 'unknown')}[/green]"
                )
            else:
                rich_print(
                    f"[yellow]⚠️  No alternative files found for {failed_file.get('filename', 'unknown')}[/yellow]"
                )
            
            return result
            
        except Exception as e:
            rich_print(
                f"[yellow]⚠️  Error searching for alternatives: {e}[/yellow]"
            )
            return []


    def add(self, criteria: Dict[str, Any], track: bool = False) -> None:
        """
        Adds or updates a query in the esgpull database.

        Args:
            criteria: A dictionary defining the query. Must include a unique 'name'.
                      Other keys can be facets (e.g., project, variable), 'limit', or 'tags'.
            track: If True, the query will be marked for tracking.
        """
        # Use the same logic as CLI add.py
        _criteria = criteria.copy()
        query_file = _criteria.pop("query_file", None)
        tags = _criteria.pop("tags", [])
        facets = [f"{k}:{v}" for k, v in _criteria.items() if k != "name"]
        name = _criteria.get("name")
        queries = []
        if query_file is not None:
            queries = serialize_queries_from_file(Path(query_file))
        else:
            query = parse_query(
                facets=facets,
                tags=tags + [f"name:{name}"] if name else tags,
                require=None,
                distrib="true",
                latest=None,
                replica=None,
                retracted=None,
            )
            self.esg.graph.resolve_require(query)
            if track:
                query.track(query.options)
            queries = [query]
        queries = self.esg.insert_default_query(*queries)
        empty = Query()
        empty.compute_sha()

        for query in queries:
            query.compute_sha()
            self.esg.graph.resolve_require(query)
            if query.sha == empty.sha:
                raise ValueError("Trying to add empty query.")
            if query.sha in self.esg.graph:
                print("Skipping existing query:", query.name)
                continue
            else:
                self.esg.graph.add(query)
                print("New query added:", query.name)
        new_queries = self.esg.graph.merge()
        if new_queries:
            print(
                f"{len(new_queries)} new query{'s' if len(new_queries) > 1 else ''} added."
            )
        else:
            print("No new query was added.")

    def valid_name_tag(
        self,
        graph: Graph,
        query_id: str | None,
        tag: str | None,
    ) -> bool:
        result = True
        # get query from id

        if query_id is not None:
            shas = graph.matching_shas(query_id, graph._shas)
            if len(shas) > 1:
                print("Multiple matches found for query ID:", query_id)
                result = False
            elif len(shas) == 0:
                print("No matches found for query ID:", query_id)
                result = False
        elif tag is not None:
            tags = [t.name for t in graph.get_tags()]
            if tag not in tags:
                print("No queries tagged with:", tag)
                result = False
        return result

    def track_query(self, query_ids: list[str]) -> None:
        """
        Marks existing queries for automatic tracking.

        Args:
            query_ids: List of query names or SHAs to track.
        """
        for sha in query_ids:
            if not self.valid_name_tag(self.esg.graph, sha, None):
                print(f"Invalid query ID: {sha}. Are you using a pre-tracked query_id?")
                continue
            query = self.esg.graph.get(sha)
            if query.tracked:
                print(f"{query.name} is already tracked.")
                continue
            if self.esg.graph.get_children(query.sha):
                print(
                    "This query has children"
                )  # TODO: handle children logic if needed
            expanded = self.esg.graph.expand(query.sha)
            tracked_query = query.clone(compute_sha=False)
            tracked_query.track(expanded.options)
            tracked_query.compute_sha()
            if tracked_query.sha == query.sha:
                # No change in SHA, just mark as tracked and merge
                query.tracked = True
                self.esg.graph.merge()
                self.esg.ui.print(f":+1: {query.name}  is now tracked.")
            else:
                self.esg.graph.replace(query, tracked_query)
                self.esg.graph.merge()
                # self.esg.ui.print(f":+1: {tracked_query.name} (previously {query.name}) is now tracked.")
                print(
                    f":+1: {tracked_query.name} (previously {query.name}) is now tracked."
                )
            return

    def untrack_query(self, query_id: str) -> None:
        """
        Unmarks an existing query from automatic tracking.

        Args:
            query_id: The name of the query to untrack.
        """
        query = self.esg.graph.get(name=query_id)
        if not query:
            raise ValueError(f"Query with id '{query_id}' not found.")
        untracked_query = query.clone()
        untracked_query.tracked = False
        self.esg.graph.replace(query, untracked_query)
        self.esg.graph.merge()

    def update(
        self, query_id: str, subset_criteria: Optional[dict] = None
    ) -> List[Dict[str, Any]]:
        """
        Updates a tracked query by searching for matching files on ESGF,
        adds new files to the database, and returns their information.

        Args:
            query_id: The name or SHA of the query to update.
            subset_criteria: Optional dict to further filter files (facet:value).

        Returns:
            A list of dictionaries representing new files added to the query.
        """
        self.esg.graph.load_db()
        query = self.esg.graph.get(name=query_id)
        if not query:
            raise ValueError(f"Query with id '{query_id}' not found.")
        if not getattr(query, "tracked", False):
            raise ValueError(
                f"Query '{query.name}' is not tracked. Only tracked queries can be updated."
            )

        expanded = self.esg.graph.expand(query.sha)
        # Search for files matching the expanded query
        files_found = self.esg.context.search(expanded, file=True)
        # Get SHAs of files already linked to the query
        existing_shas = {getattr(f, "sha", None) for f in getattr(query, "files", [])}
        # Filter out files already linked
        new_files = []
        for file in files_found:
            if getattr(file, "sha", None) not in existing_shas:
                if subset_criteria and not all(
                    getattr(file, k, None) == v for k, v in subset_criteria.items()
                ):
                    continue
                new_files.append(file)
        # Link new files to the query in the DB
        if new_files:
            for file in new_files:
                file.status = getattr(file, "status", None) or File.Status.Queued
                self.esg.db.session.add(file)
                self.esg.db.link(query=query, file=file)
            query.updated_at = datetime.now(timezone.utc)
            self.esg.db.session.add(query)
            self.esg.db.session.commit()
        self.esg.graph.merge()
        return [f.asdict() for f in new_files]

    def download(self, query_id: str) -> List[Dict[str, Any]]:
        """
        Downloads files associated with a given query that are in 'queued' state.

        Args:
            query_id: The name of the query for which to download files.

        Returns:
            A list of dictionaries representing the files processed by the download command,
            including their status.
        """
        # Use the same logic as CLI download.py (simplified)
        query = self.esg.graph.get(name=query_id)
        if not query:
            raise ValueError(f"Query with id '{query_id}' not found.")
        files_to_download = self.esg.db.get_files_by_query(query, status=None)
        if not files_to_download:
            return []
        downloaded_files, error_files = sync(
            self.esg.download(files_to_download, show_progress=False)
        )
        all_processed_files = downloaded_files + [
            err.data for err in error_files if hasattr(err, "data")
        ]
        return [cast(Dict[str, Any], f.asdict()) for f in all_processed_files]

    def list_queries(self) -> List[Dict[str, Any]]:
        """
        Lists all queries in the esgpull database.

        Returns:
            A list of dictionaries, where each dictionary represents a query.
        """
        self.esg.graph.load_db()  # load queries from db (otherwise inaccessible)
        return [
            {"name": q.name, "sha": q.sha, **q.asdict()}
            for q in self.esg.graph.queries.values()
        ]

    def get_query(self, query_id: str) -> Optional[Dict[str, Any]]:
        """
        Retrieves a specific query by its name.

        Args:
            query_id: The name of the query.

        Returns:
            A dictionary representing the query if found, otherwise None.
        """
        self.esg.graph.load_db()
        query = self.esg.graph.get(name=query_id)
        if query:
            return {"name": query.name, "sha": query.sha, **query.asdict()}
        return None


def search_and_download(search_criteria, meta_criteria, API=None):
    """
    Perform a search and download files based on the provided criteria.
    Uses batching to avoid UI issues with large numbers of files.
    """
    # Check for shutdown request
    if _shutdown_requested.is_set():
        rich_print("[yellow]Shutdown requested, skipping search and download.[/yellow]")
        return

    # load configuration
    search_results = search.SearchResults(
        search_criteria=search_criteria,
        meta_criteria=meta_criteria,
    )
    
    # Check system resources before starting
    if meta_criteria.get("test", True):
        # output_dir = meta_criteria.get("output_dir", None)
        output_dir = "test_downloads"
    else:
        output_dir = meta_criteria.get("output_dir", None)
    if output_dir:
        search_results.check_system_resources(output_dir)
    
    files = search_results.run()

    if not files:
        rich_print("No files found matching the search criteria.")
        return
    
    # Determine batch size based on system resources and file count
    requested_batch_size = meta_criteria.get("batch_size", 50)  # Default batch size
    adaptive_batch_size = search_results._get_adaptive_batch_size(requested_batch_size, len(files))
    
    if adaptive_batch_size != requested_batch_size:
        rich_print(f"[blue]Adapted batch size from {requested_batch_size} to {adaptive_batch_size} based on system resources[/blue]")

    # Process files in batches
    total_files = len(files)
    total_batches = (total_files + adaptive_batch_size - 1) // adaptive_batch_size
    total_files_str = "files" if total_files > 1 else "file"
    batch_str = "batches" if total_batches > 1 else "batch"
    batch_size_str = "files" if adaptive_batch_size > 1 else "file"
    rich_print(f"[blue]📥 Processing {total_files} {total_files_str} in {total_batches} {batch_str} of up to {adaptive_batch_size} {batch_size_str} each...[/blue]")
    
    try:
        if API is None:
            API = api.EsgpullAPI()
        
        for batch_num in range(total_batches):
            # Check for shutdown request at start of each batch
            if _shutdown_requested.is_set():
                rich_print("[yellow]Shutdown requested, stopping batch processing.[/yellow]")
                break
                
            start_idx = batch_num * adaptive_batch_size
            end_idx = min(start_idx + adaptive_batch_size, total_files)
            batch_files = files[start_idx:end_idx]
            
            rich_print(f"[blue]📦 Processing batch {batch_num + 1}/{total_batches} ({len(batch_files)} files)...[/blue]")
            
            try:
                find_alternatives = meta_criteria.get("find_alternatives", True)
                download_manager = download.DownloadSubset(
                    files=batch_files,
                    fs=API.esg.fs,
                    output_dir=output_dir,
                    subset=meta_criteria.get("subset"),
                    max_workers=meta_criteria.get("max_workers", 32),
                    verbose=meta_criteria.get("verbose", False),
                    find_alternatives=find_alternatives,
                    api_instance=API if find_alternatives else None,
                )

                # Pass shutdown event to download manager
                download_manager._shutdown_requested = _shutdown_requested
                download_manager.run()
                
                rich_print(f"[green]✅ Batch {batch_num + 1}/{total_batches} completed successfully[/green]")
                
            except KeyboardInterrupt:
                rich_print("[yellow]Download interrupted by user.[/yellow]")
                raise
            except Exception as e:
                rich_print(f"[red]Batch {batch_num + 1} failed with error: {e}[/red]")
                # Continue with next batch instead of failing completely
                continue

    except KeyboardInterrupt:
        rich_print("[yellow]Download interrupted by user.[/yellow]")
        raise
    except Exception as e:
        rich_print(f"[red]Download failed with error: {e}[/red]")
        raise

def remove_part_files(main_dir: Path) -> None:
    """
    Remove part files from the main directory.
    This is useful for cleaning up temporary files after downloads.
    """
    part_files = list(main_dir.glob("*.part"))
    if not part_files:
        rich_print("No .part files found to remove.")
        return

    for part_file in part_files:
        try:
            part_file.unlink()
            rich_print(f"Removed .part file: {part_file}")
        except Exception as e:
            rich_print(f"Failed to remove {part_file}: {e}")


def run():
    """Main run function with graceful interrupt handling."""
    with GracefulInterruptHandler():
        try:
            # Show startup message with interrupt info
            rich_print(
                "[dim]💡 Tip: Press Ctrl+C at any time for graceful shutdown[/dim]"
            )

            REPO_ROOT = fileops.get_repo_root()
            CRITERIA_DICT = fileops.read_yaml(REPO_ROOT / "search.yaml")
            SEARCH_CRITERIA_CONFIG = CRITERIA_DICT.get("search_criteria", {})
            META_CRITERIA_CONFIG = CRITERIA_DICT.get("meta_criteria", {})
            API = api.EsgpullAPI()

            # remove any existing .part files in the main directory to avoid conflicts
            remove_part_files(API.esg.fs.paths.data)

            # Convert comma-separated strings to lists for iteration
            exp_str = SEARCH_CRITERIA_CONFIG.get("experiment_id", "")
            experiments = [e.strip() for e in exp_str.split(",") if e.strip()]

            var_str = SEARCH_CRITERIA_CONFIG.get("variable", "")
            variables = [v.strip() for v in var_str.split(",") if v.strip()]

            # Use placeholders to ensure loops run at least once, even if no values are specified
            iter_exps = experiments if experiments else [None]
            iter_vars = variables if variables else [None]

            total_combinations = len(iter_vars) * len(iter_exps)
            current_combination = 0

            for var in iter_vars:
                for exp in iter_exps:
                    # Check for shutdown request at start of each iteration
                    if _shutdown_requested.is_set():
                        rich_print(
                            "[yellow]Shutdown requested, stopping processing.[/yellow]"
                        )
                        return

                    current_combination += 1
                    search_criteria = SEARCH_CRITERIA_CONFIG.copy()
                    print_parts = []

                    # Override criteria and build print message only if iterating on that criterion
                    if exp is not None:
                        search_criteria["experiment_id"] = exp
                        print_parts.append(
                            f":test_tube: [bold]Experiment:[/bold] {exp.upper()}"
                        )

                    if var is not None:
                        search_criteria["variable"] = var
                        print_parts.append(
                            f":mag: [bold]Variable:[/bold] {var.upper()}"
                        )

                    # Show progress
                    progress_msg = (
                        f"[dim]({current_combination}/{total_combinations})[/dim]"
                    )

                    if print_parts:
                        rich_print(f"\n{progress_msg} " + " ".join(print_parts))
                    else:
                        rich_print(
                            f"\n{progress_msg} :mag: [bold]Searching with specified criteria...[/bold]"
                        )

                    try:
                        search_and_download(
                            search_criteria=search_criteria,
                            meta_criteria=META_CRITERIA_CONFIG,
                            API=API,
                        )
                    except KeyboardInterrupt:
                        rich_print("[yellow]Processing interrupted by user.[/yellow]")
                        break
                    except Exception as e:
                        import traceback
                        rich_print(f"[red]Error processing combination: {e}[/red]")
                        rich_print(f"[red]{traceback.format_exc()}[/red]")
                        continue

                # Break outer loop if interrupted
                if _shutdown_requested.is_set():
                    break

            # # Check if regridding should run (only if not interrupted)
            # if not _shutdown_requested.is_set() and META_CRITERIA_CONFIG.get(
            #     "regrid", False
            # ):
            #     rich_print("[blue]Regridding enabled, running regridding...[/blue]")
            #     try:
            #         regrid.RegridderManager(
            #             fs=API.esg.fs,
            #             watch_dir=API.esg.fs.data,
            #         )
            #     except KeyboardInterrupt:
            #         rich_print("[yellow]Regridding interrupted by user.[/yellow]")
            #     except Exception as e:
            #         rich_print(f"[red]Regridding failed: {e}[/red]")

            # if _shutdown_requested.is_set():
            #     rich_print(
            #         "\n[bold green]✅ Graceful shutdown completed successfully.[/bold green]"
            #     )
            #     rich_print(
            #         "[dim]All downloads stopped cleanly. No data was lost.[/dim]"
            #     )
            # else:
            #     rich_print(
            #         "\n[bold green]✅ Processing completed successfully.[/bold green]"
            #     ) if META_CRITERIA_CONFIG["process"] else rich_print(
            #         "\n[bold green]✅ No processing executed (none specified).[/bold green]"
            #     )

        except KeyboardInterrupt:
            rich_print("[yellow]Main process interrupted.[/yellow]")
        except Exception as e:
            rich_print(f"[red]Unexpected error in main process: {e}[/red]")
            raise


if __name__ == "__main__":
    run()
