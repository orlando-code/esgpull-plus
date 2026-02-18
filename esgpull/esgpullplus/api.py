import re
import signal
import sys
import threading
import time
from pathlib import Path
from typing import Any, Dict, List, Optional, cast
from datetime import datetime, timezone

from rich import print as rich_print


from esgpull.cli.utils import (
    parse_facets,
    parse_query,
    serialize_queries_from_file,
)
# TO DO
# XX processing box

# custom
from esgpull.esgpullplus import download, fileops, search, esgpuller
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

    def search(
        self,
        criteria: Optional[Dict[str, Any]] = None,
        facets: Optional[List[str]] = None,
        file: bool = False,
    ) -> List[Dict[str, Any]]:
        """
        Searches ESGF nodes for datasets or files matching the criteria.
        Aligns with CLI: default file=False (dataset search, faster).

        Args:
            criteria (dict, optional): A dictionary of search facets (e.g., project, variable).
                Can include 'limit' to restrict the number of results.
            facets (list[str], optional): CLI-style facet strings (e.g. project:CMIP6 variable:tas
                experiment_id:historical table_id:Amon frequency:mon). Same format as esgpull search.
                If provided, takes precedence over criteria.
            file (bool): If True, search for files (type=File); if False, search for datasets
                (type=Dataset). Default False matches CLI default - dataset search is faster
                (fewer records, same metadata for source analysis).

        Returns:
            A list of dictionaries, where each dictionary represents a found file or dataset.
        """
        if facets is not None:
            criteria = self._facets_to_criteria(facets)
        if criteria is None:
            criteria = {}
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
            from esgpull.esgpullplus.enhanced_context import EnhancedContext
            local_ctx = EnhancedContext(config=self.esg.config, noraise=True)
            results = local_ctx.search(query, file=file, max_hits=max_hits)
            # Dataset results are already dicts; file results are EnhancedFile
            return [
                r.asdict() if hasattr(r, "asdict") else r
                for r in results
            ]
        except (IndexError, ValueError) as e:
            # Handle empty hits that cause IndexError in _distribute_hits_impl
            error_msg = str(e).lower()
            if "index" in error_msg or "list index out of range" in error_msg or "list index" in error_msg:
                # Empty hits - return empty results gracefully
                rich_print(
                    "[yellow]⚠️  Search returned no results (empty hits). "
                    "This may indicate the search criteria matched no files.[/yellow]"
                )
                return []
            raise
        except Exception as e:
            # If distributed search fails with 500 error, try alternative nodes
            if self._is_server_error(e):
                rich_print(
                    "[yellow]⚠️  Distributed search failed with server error. "
                    "Retrying with alternative ESGF nodes...[/yellow]"
                )
                return self._search_with_retry(_criteria, tags, max_hits, file=file)
            raise
    
    def _facets_to_criteria(self, facets: List[str]) -> Dict[str, Any]:
        """
        Convert CLI-style facet strings to a criteria dict for search().
        Example: ["project:CMIP6", "variable_id:tas", "experiment_id:ssp*"]
        -> {"project": "CMIP6", "variable_id": "tas", "experiment_id": "ssp*"}
        """
        selection = parse_facets(facets)
        return {k: ",".join(v) if len(v) > 1 else v[0] for k, v in selection.items()}

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
        file: bool = False,
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
                rich_print(f"[cyan]🔍 Trying ESGF node: {node}[/cyan]")
                from esgpull.esgpullplus.enhanced_context import EnhancedContext
                self.esg.config.api.index_node = node
                local_ctx = EnhancedContext(config=self.esg.config, noraise=True)
                results = local_ctx.search(query, file=file, max_hits=max_hits)
                rich_print(f"[green]✅ Successfully searched using node: {node}[/green]")
                # Restore original node
                self.esg.config.api.index_node = original_node
                return [
                    r.asdict() if hasattr(r, "asdict") else r
                    for r in results
                ]
                    
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
            f"[cyan]🔍 Searching for alternative files for {failed_file.get('filename', 'unknown file')}...[/cyan]"
        )
        
        try:
            alternatives = self.search(search_criteria)
            
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


def _parse_year_range_from_filename(filename: str) -> Optional[tuple[int, int]]:
    """
    Parse file time range from CMIP-style filename (e.g. ..._201501-202012.nc or ..._185001-185512.nc).
    Returns (file_start_year, file_end_year) or None if pattern not found.
    """
    if not filename:
        return None
    match = re.search(r"_(\d{6})-(\d{6})(?:\.[^.]+)?$", filename.strip())
    if not match:
        return None
    try:
        start_ym = int(match.group(1))  # YYYYMM
        end_ym = int(match.group(2))
        return (start_ym // 100, end_ym // 100)
    except (ValueError, TypeError):
        return None


def _file_in_year_range(
    file_obj: Any,
    start_year: Optional[int],
    end_year: Optional[int],
) -> bool:
    """
    Return True if the file's time range passes the year filter.

    - Both start_year and end_year: keep if file range overlaps [start_year, end_year].
    - Only end_year: keep if file ends on or before end_year (file_end_year <= end_year).
    - Only start_year: keep if file starts on or after start_year (file_start_year >= start_year).

    File range is parsed from filename (_YYYYMM-YYYYMM). If unparseable, keep the file (assume in range).
    """
    filename = getattr(file_obj, "filename", None)
    if filename is None and hasattr(file_obj, "asdict"):
        filename = file_obj.asdict().get("filename")
    if not filename:
        return True
    parsed = _parse_year_range_from_filename(str(filename))
    if parsed is None:
        return True
    file_start_year, file_end_year = parsed
    if start_year is not None and end_year is not None:
        return file_end_year >= start_year and file_start_year <= end_year
    if end_year is not None:
        return file_end_year <= end_year
    if start_year is not None:
        return file_start_year >= start_year
    return True


def search_and_download(search_criteria, meta_criteria, API=None, symmetrical=False, symmetrical_sources_cache=None):
    """
    Perform a search and download files based on the provided criteria.
    Uses batching to avoid UI issues with large numbers of files.
    
    Args:
        search_criteria: Dictionary of search criteria
        meta_criteria: Dictionary of metadata criteria (may include start_year, end_year for time filter)
        API: Optional EsgpullAPI instance
        symmetrical: If True, only download files from sources that have both 
                    historical and SSP experiments available
        symmetrical_sources_cache: Pre-computed cache of symmetrical sources (from full search)
    """
    # Check for shutdown request
    if _shutdown_requested.is_set():
        rich_print("[yellow]Shutdown requested, skipping search and download.[/yellow]")
        return

    # load configuration - use file=True for downloads (need url, filename, local_path)
    # dataset search (file=False) is for analysis only; download needs file-level metadata
    search_results = search.SearchResults(
        search_criteria=search_criteria,
        meta_criteria=meta_criteria,
        file=True,
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

    # Filter by year range (meta_criteria start_year / end_year) if either is set
    start_year = meta_criteria.get("start_year")
    end_year = meta_criteria.get("end_year")
    if start_year is not None:
        try:
            start_year = int(start_year)
        except (TypeError, ValueError):
            start_year = None
    if end_year is not None:
        try:
            end_year = int(end_year)
        except (TypeError, ValueError):
            end_year = None
    if start_year is not None or end_year is not None:
        original_count = len(files)
        files = [f for f in files if _file_in_year_range(f, start_year, end_year)]
        removed = original_count - len(files)
        if removed > 0:
            if start_year is not None and end_year is not None:
                range_msg = f"[{start_year}-{end_year}]"
            elif end_year is not None:
                range_msg = f"before or in {end_year} (end_year)"
            else:
                range_msg = f"from {start_year} onward (start_year)"
            rich_print(
                f"[cyan]📅 Time filter {range_msg}: removed {removed} file(s) outside range "
                f"({len(files)} remaining)[/cyan]"
            )
        if not files:
            rich_print("No files remaining after time range filter.")
            return

    # Filter for symmetrical datasets if requested
    if symmetrical:
        rich_print("[cyan]🔍 Filtering for symmetrical datasets (sources with both historical and SSP experiments)...[/cyan]")
        try:
            # Use pre-computed cache if available, otherwise analyze current results
            if symmetrical_sources_cache is not None:
                rich_print("[dim]📊 Using pre-computed symmetrical sources cache[/dim]")
                symmetrical_sources = symmetrical_sources_cache['with_var']
                symmetrical_sources_no_var = symmetrical_sources_cache['without_var']
                
                if not symmetrical_sources and not symmetrical_sources_no_var:
                    rich_print("[yellow]⚠️  No symmetrical sources in cache. No files to download.[/yellow]")
                    return
            else:
                # Fallback: analyze current results (may not work if only one experiment)
                rich_print("[yellow]⚠️  No symmetrical sources cache available. Analyzing current search results...[/yellow]")
                rich_print("[yellow]⚠️  Note: This may fail if current search only includes one experiment type.[/yellow]")
                
                # Ensure we have results_df populated
                if search_results.results_df is None or search_results.results_df.empty:
                    rich_print("[yellow]⚠️  No search results available for symmetry analysis. Cannot filter.[/yellow]")
                    return
                
                # Analyze source availability to find sources with both historical and SSP
                analysis_df = search_results.analyze_source_availability(
                    historical_experiment="historical",
                    ssp_pattern="ssp",
                    require_both=True,
                )
                
                if analysis_df.empty:
                    rich_print("[yellow]⚠️  No sources found with both historical and SSP experiments. No files to download.[/yellow]")
                    return
                
                # Normalize values to ensure consistent matching
                def normalize_value(val):
                    """Normalize a value for consistent matching."""
                    if val is None:
                        return None
                    try:
                        if isinstance(val, float) and val != val:
                            return None
                    except (TypeError, ValueError):
                        pass
                    try:
                        return str(val).strip()
                    except (TypeError, ValueError):
                        return None
                
                symmetrical_sources = set()
                symmetrical_sources_no_var = set()
                
                for _, row in analysis_df.iterrows():
                    var = normalize_value(row.get("variable", None))
                    inst_id = normalize_value(row["institution_id"])
                    src_id = normalize_value(row["source_id"])
                    
                    if inst_id and src_id:
                        if var:
                            symmetrical_sources.add((var, inst_id, src_id))
                        symmetrical_sources_no_var.add((inst_id, src_id))
            
            rich_print(f"[green]✅ Using {len(symmetrical_sources)} symmetrical source(s) for filtering[/green]")
            
            # Normalize function for file attributes
            def normalize_value(val):
                """Normalize a value for consistent matching."""
                if val is None:
                    return None
                try:
                    if isinstance(val, float) and val != val:
                        return None
                except (TypeError, ValueError):
                    pass
                try:
                    return str(val).strip()
                except (TypeError, ValueError):
                    return None
            
            # Filter files to only include those from symmetrical sources
            original_count = len(files)
            filtered_files = []
            skipped_count = 0
            skipped_details = {}
            
            for file in files:
                # Get attributes from the file
                inst_id = None
                src_id = None
                var = None
                
                if hasattr(file, 'institution_id') and hasattr(file, 'source_id'):
                    inst_id = getattr(file, 'institution_id', None)
                    src_id = getattr(file, 'source_id', None)
                    var = getattr(file, 'variable', None)
                elif hasattr(file, 'asdict'):
                    # Fallback: convert to dict and check
                    file_dict = file.asdict()
                    inst_id = file_dict.get("institution_id")
                    src_id = file_dict.get("source_id")
                    var = file_dict.get("variable")
                
                # Normalize values for consistent matching
                inst_id = normalize_value(inst_id)
                src_id = normalize_value(src_id)
                var = normalize_value(var)
                
                if inst_id and src_id:
                    # Try matching with variable first
                    if var:
                        source_key = (var, inst_id, src_id)
                        if source_key in symmetrical_sources:
                            filtered_files.append(file)
                            continue
                    
                    # Fallback: try matching without variable
                    source_key_no_var = (inst_id, src_id)
                    if source_key_no_var in symmetrical_sources_no_var:
                        filtered_files.append(file)
                        continue
                    
                    # Track why files are being skipped for debugging
                    skip_key = f"{inst_id}/{src_id}"
                    if skip_key not in skipped_details:
                        skipped_details[skip_key] = 0
                    skipped_details[skip_key] += 1
                    skipped_count += 1
                else:
                    # File doesn't have institution_id or source_id - skip it
                    skipped_count += 1
            
            files = filtered_files
            filtered_count = len(files)
            
            source_str = "sources" if filtered_count > 1 else "source"
            # Debug output
            if skipped_details:
                rich_print(f"[dim]📊 Skipped {len(skipped_details)} unique {source_str}: {list(skipped_details.keys())[:5]}...[/dim]")
            
            if filtered_count < original_count:
                rich_print(
                    f"[cyan]📊 Filtered from {original_count} to {filtered_count} files "
                    f"({original_count - filtered_count} removed from non-symmetrical {source_str})[/cyan]"
                )
            elif filtered_count == original_count and original_count > 0:
                rich_print(f"[green]✅ All {filtered_count} files are from symmetrical {source_str}[/green]")
            else:
                rich_print(f"[yellow]⚠️  No files remaining after filtering[/yellow]")
                return
                
        except Exception as e:
            rich_print(f"[yellow]⚠️  Error filtering for symmetrical datasets: {e}[/yellow]")
            rich_print("[yellow]⚠️  Proceeding with all files (no filtering applied)[/yellow]")
            import traceback
            traceback.print_exc()
    
    # Determine batch size based on system resources and file count
    requested_batch_size = meta_criteria.get("batch_size", 50)  # Default batch size
    adaptive_batch_size = search_results._get_adaptive_batch_size(requested_batch_size, len(files))
    
    if adaptive_batch_size != requested_batch_size:
        rich_print(f"[cyan]Adapted batch size from {requested_batch_size} to {adaptive_batch_size} based on system resources[/cyan]")

    # Process files in batches
    total_files = len(files)
    total_batches = (total_files + adaptive_batch_size - 1) // adaptive_batch_size
    total_files_str = "files" if total_files > 1 else "file"
    batch_str = "batches" if total_batches > 1 else "batch"
    batch_size_str = "files" if adaptive_batch_size > 1 else "file"
    rich_print(f"[cyan]📥 Processing {total_files} {total_files_str} in {total_batches} {batch_str} of up to {adaptive_batch_size} {batch_size_str} each...[/cyan]")
    
    try:
        if API is None:
            API = EsgpullAPI()
        
        for batch_num in range(total_batches):
            # Check for shutdown request at start of each batch
            if _shutdown_requested.is_set():
                rich_print("[yellow]Shutdown requested, stopping batch processing.[/yellow]")
                break
                
            start_idx = batch_num * adaptive_batch_size
            end_idx = min(start_idx + adaptive_batch_size, total_files)
            batch_files = files[start_idx:end_idx]
            
            rich_print(f"[cyan]📦 Processing batch {batch_num + 1}/{total_batches} ({len(batch_files)} files)...[/cyan]")
            
            try:
                find_alternatives = meta_criteria.get("find_alternatives", True)
                data_dir = meta_criteria.get("data_dir")
                download_manager = download.DownloadSubset(
                    files=batch_files,
                    fs=API.esg.fs,
                    output_dir=output_dir,
                    data_dir=data_dir,
                    subset=meta_criteria.get("subset"),
                    max_workers=meta_criteria.get("max_workers", 32),
                    verbose=meta_criteria.get("verbose", False),
                    find_alternatives=find_alternatives,
                    api_instance=API if find_alternatives else None,
                )

                # Pass shutdown event to download manager
                download_manager._shutdown_requested = _shutdown_requested
                download_manager.run()
                
                rich_print(f"[green]✅ Batch {batch_num + 1}/{total_batches} completed[/green]")
                
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
    Remove part files from the main directory (searches recursively).
    This is useful for cleaning up temporary files after downloads.
    """
    part_files = list(main_dir.glob("**/*.part"))
    if not part_files:
        rich_print("No .part files found to remove.")
        return

    for part_file in part_files:
        try:
            part_file.unlink()
            rich_print(f"Removed .part file: {part_file}")
        except Exception as e:
            rich_print(f"Failed to remove {part_file}: {e}")


def run(symmetrical: bool = False):
    """
    Main run function with graceful interrupt handling.
    
    Args:
        symmetrical: If True, only download files from sources that have both 
                     historical and SSP experiments available. Defaults to False.
                     When called from CLI, this is overridden by --symmetrical flag.
    """
    import argparse
    import sys
    
    # Only parse command-line arguments if called from CLI (not from notebook/REPL)
    # Check if we're in IPython/Jupyter by looking for IPython in sys.modules
    is_notebook = 'IPython' in sys.modules or 'ipykernel' in sys.modules
    
    if not is_notebook and len(sys.argv) > 1:
        # Parse command-line arguments
        parser = argparse.ArgumentParser(
            description="ESGF search and download tool with symmetrical dataset filtering"
        )
        parser.add_argument(
            "--symmetrical",
            action="store_true",
            help="Only download files from sources that have both historical and SSP experiments available (symmetrical datasets)",
        )
        args = parser.parse_args()
        symmetrical = args.symmetrical
    
    with GracefulInterruptHandler():
        try:
            # Show startup message with interrupt info
            rich_print(
                "[dim]💡 Tip: Press Ctrl+C at any time for graceful shutdown[/dim]"
            )
            
            if symmetrical:
                rich_print(
                    "[cyan]🔍 Symmetrical mode enabled: Only downloading datasets with both historical and SSP experiments[/cyan]"
                )

            REPO_ROOT = fileops.get_repo_root()
            print("REPO ROOT:", REPO_ROOT)
            CRITERIA_DICT = fileops.read_yaml(REPO_ROOT / "search.yaml")
            SEARCH_CRITERIA_CONFIG = CRITERIA_DICT.get("search_criteria", {})
            META_CRITERIA_CONFIG = CRITERIA_DICT.get("meta_criteria", {})
            API = EsgpullAPI()

            # remove any existing .part files in the main directory to avoid conflicts
            data_dir = META_CRITERIA_CONFIG.get("data_dir")
            main_dir = Path(data_dir) if data_dir else API.esg.fs.paths.data
            remove_part_files(main_dir)

            # Convert comma-separated strings to lists for iteration
            exp_str = SEARCH_CRITERIA_CONFIG.get("experiment_id", "")
            experiments = [e.strip() for e in exp_str.split(",") if e.strip()]

            var_str = SEARCH_CRITERIA_CONFIG.get("variable", "")
            variables = [v.strip() for v in var_str.split(",") if v.strip()]

            # If symmetrical mode, first determine which sources have both historical and SSP
            symmetrical_sources_cache = None
            if symmetrical:
                rich_print("[cyan]🔍 Pre-analyzing symmetrical sources (searching all experiments)...[/cyan]")
                try:
                    # Create a search with ALL experiments to determine symmetry
                    full_search_criteria = SEARCH_CRITERIA_CONFIG.copy()
                    # Don't filter by individual experiment - use all experiments
                    # The search will include all experiments from the original criteria
                    
                    full_search_results = search.SearchResults(
                        search_criteria=full_search_criteria,
                        meta_criteria=META_CRITERIA_CONFIG,
                    )
                    
                    # Run the full search to get all results
                    _ = full_search_results.run()  # Get all files, but we only need results_df
                    
                    if full_search_results.results_df is not None and not full_search_results.results_df.empty:
                        # Analyze for symmetrical sources
                        analysis_df = full_search_results.analyze_source_availability(
                            historical_experiment="historical",
                            ssp_pattern="ssp",
                            require_both=True,
                        )
                        
                        if not analysis_df.empty:
                            # Build cache of symmetrical sources
                            def normalize_value(val):
                                if val is None:
                                    return None
                                try:
                                    if isinstance(val, float) and val != val:
                                        return None
                                except (TypeError, ValueError):
                                    pass
                                try:
                                    return str(val).strip()
                                except (TypeError, ValueError):
                                    return None
                            
                            symmetrical_sources_cache = {
                                'with_var': set(),
                                'without_var': set()
                            }
                            
                            for _, row in analysis_df.iterrows():
                                var = normalize_value(row.get("variable", None))
                                inst_id = normalize_value(row["institution_id"])
                                src_id = normalize_value(row["source_id"])
                                
                                if inst_id and src_id:
                                    if var:
                                        symmetrical_sources_cache['with_var'].add((var, inst_id, src_id))
                                    symmetrical_sources_cache['without_var'].add((inst_id, src_id))
                            
                            rich_print(
                                f"[green]✅ Found {len(symmetrical_sources_cache['with_var'])} symmetrical source(s) "
                                f"(variable+institution+source combinations)[/green]"
                            )
                        else:
                            rich_print("[yellow]⚠️  No symmetrical sources found in full search. Symmetry filtering will be skipped.[/yellow]")
                    else:
                        rich_print("[yellow]⚠️  No results from full search. Symmetry filtering will be skipped.[/yellow]")
                except Exception as e:
                    rich_print(f"[yellow]⚠️  Error pre-analyzing symmetrical sources: {e}[/yellow]")
                    rich_print("[yellow]⚠️  Symmetry filtering will be skipped.[/yellow]")
                    import traceback
                    traceback.print_exc()

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
                            symmetrical=symmetrical,
                            symmetrical_sources_cache=symmetrical_sources_cache,
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

            # Run post-download regridding if configured
            if not _shutdown_requested.is_set() and META_CRITERIA_CONFIG.get("regrid", False):
                rich_print("[cyan]Regridding enabled, running regridding...[/cyan]")
                try:
                    from esgpull.esgpullplus.cdo_regrid import regrid_directory
                    target_res = META_CRITERIA_CONFIG.get("regrid_resolution", (1.0, 1.0))
                    if isinstance(target_res, (list, tuple)) and len(target_res) == 2:
                        target_res = tuple(target_res)
                    else:
                        target_res = (1.0, 1.0)
                    regrid_input = Path(META_CRITERIA_CONFIG["data_dir"]) if META_CRITERIA_CONFIG.get("data_dir") else API.esg.fs.paths.data
                    regrid_directory(
                        input_dir=regrid_input,
                        include_subdirectories=True,
                        target_resolution=target_res,
                        max_workers=META_CRITERIA_CONFIG.get("max_workers", 4),
                    )
                except KeyboardInterrupt:
                    rich_print("[yellow]Regridding interrupted by user.[/yellow]")
                except Exception as e:
                    rich_print(f"[red]Regridding failed: {e}[/red]")

            if _shutdown_requested.is_set():
                rich_print("\n[bold green]Graceful shutdown completed.[/bold green]")
            else:
                rich_print("\n[bold green]Processing completed successfully.[/bold green]")

        except KeyboardInterrupt:
            rich_print("[yellow]Main process interrupted.[/yellow]")
        except Exception as e:
            rich_print(f"[red]Unexpected error in main process: {e}[/red]")
            raise


if __name__ == "__main__":
    run()
