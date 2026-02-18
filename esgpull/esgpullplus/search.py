# general
import logging
import os
import re
from pathlib import Path
from typing import Optional

import numpy as np
import pandas as pd
from rich.console import Console
from rich.panel import Panel
import httpx

# rich
from rich.table import Table

# custom
from esgpull.esgpullplus import api, fileops, config, utils
from esgpull.esgpullplus.enhanced_file import EnhancedFile

log = logging.getLogger(__name__)
    

class SearchResults:
    """
    A class to hold search results from the Esgpull API.
    It can be used to filter, sort, and manipulate the results.
    """

    def __init__(
        self,
        search_criteria: Optional[dict] = None,
        meta_criteria: Optional[dict] = None,
        config_path: Optional[str] = None,
        file: bool = False,
    ):
        # Validate and set search_criteria
        if search_criteria is None:
            self.search_criteria = {}
        elif isinstance(search_criteria, dict):
            self.search_criteria = search_criteria
        else:
            raise TypeError(
                f"search_criteria must be a dict, got {type(search_criteria).__name__}. "
                f"Did you mean to pass a dictionary like {{'project': 'CMIP6'}} instead of {search_criteria}?"
            )
        
        # Validate and set meta_criteria
        if meta_criteria is None:
            self.meta_criteria = {}
        elif isinstance(meta_criteria, dict):
            self.meta_criteria = meta_criteria
        else:
            raise TypeError(
                f"meta_criteria must be a dict, got {type(meta_criteria).__name__}. "
                f"Did you mean to pass a dictionary like {{'test': False}} instead of {meta_criteria}?"
            )
        
        self.search_filter = self.search_criteria.get("filter", {})
        self.top_n = self.search_filter.get("top_n", None)
        self.limit = self.search_filter.get("limit", 4)   # limit results to return, good for debugging
        self.search_results = []  # List to hold EnhancedFile objects from search
        self.results_df = None  # DataFrame to hold results for further processing
        self.results_df_top = None  # DataFrame for top N results from search
        self.fs = api.EsgpullAPI().esg.fs  # File system from Esgpull API
        self._file_search = file  # False = dataset search (faster), True = file search
        # Cache within data_dir when set, else under esgpull data
        data_dir = Path(meta_criteria.get("data_dir")) if meta_criteria and meta_criteria.get("data_dir") else None
        self.search_results_dir = (data_dir / "search_results") if data_dir else (self.fs.paths.data / "search_results")

    def load_config(self, config_path: str) -> None:
        """Load search criteria and metadata from a YAML configuration file."""
        config = fileops.read_yaml(config_path)
        self.search_criteria = config.get("search_criteria", {})
        self.meta_criteria = config.get("meta_criteria", {})
        self.search_filter = self.search_criteria.get("filter", {})
        # Update search_results_dir if data_dir in meta_criteria
        data_dir = Path(self.meta_criteria.get("data_dir")) if self.meta_criteria.get("data_dir") else None
        if data_dir:
            self.search_results_dir = data_dir / "search_results"
        self.top_n = self.search_filter.get("top_n", None)  # get top n of grouped data ie. first n models from ensemble
        self.limit = self.search_filter.get("limit", 4)   # good for debugging

    def do_search(self) -> None:
        """Perform a search using the provided criteria and populate results with enhanced metadata."""
        api_instance = api.EsgpullAPI()
        
        # Use enhanced search to get all available metadata (file=False = dataset search, faster)
        try:
            results = api_instance.search(criteria=self.search_criteria, file=self._file_search)
        except ExceptionGroup as eg:
            # Handle ExceptionGroup from ESGF API errors
            error_messages = []
            for exc in eg.exceptions:
                if isinstance(exc, httpx.HTTPStatusError):
                    status_code = exc.response.status_code
                    url = str(exc.request.url) if hasattr(exc, 'request') and exc.request else "unknown"
                    error_messages.append(
                        f"ESGF server error {status_code} for {url}. "
                        f"This is a server-side issue at the ESGF node, not a problem with your search criteria."
                    )
                else:
                    error_messages.append(str(exc))
            
            error_msg = "\n".join(error_messages)
            raise RuntimeError(
                f"Failed to search ESGF: {error_msg}\n"
                f"Search criteria: {self.search_criteria}"
            ) from eg
        except httpx.HTTPStatusError as e:
            status_code = e.response.status_code
            url = str(e.request.url) if hasattr(e, 'request') and e.request else "unknown"
            raise RuntimeError(
                f"ESGF server error {status_code} for {url}. "
                f"This is a server-side issue at the ESGF node, not a problem with your search criteria. "
                f"Search criteria: {self.search_criteria}"
            ) from e
        except Exception as e:
            raise RuntimeError(
                f"Error during ESGF search: {e}\n"
                f"Search criteria: {self.search_criteria}"
            ) from e
        
        # Convert results to enhanced file dictionaries for future processing
        enhanced_results = []
        for result in results:
            if isinstance(result, dict):
                # result is already enhanced from search_enhanced
                enhanced_results.append(result)
            else:
                # Fallback: create enhanced file from result
                enhanced_file = EnhancedFile.fromdict(result)
                enhanced_results.append(enhanced_file.asdict())
        
        self.results_df = pd.DataFrame(enhanced_results)
        if not self.results_df.empty:
            return self.sort_results_by_metadata()
        else:
            log.info("No results found for given criteria.")

    def sort_results_by_metadata(self) -> None:
        """Sort a list of EnhancedFile objects by institution_id, source_id, experiment_id, member_id."""
        if self.results_df is None or self.results_df.empty:
            log.info("No results to sort.")
            return
        # convert resolutions to float for sorting
        resolutions = self.results_df.apply(
            lambda f: utils.calc_resolution(f.nominal_resolution), axis=1
        )
        self.results_df["resolution"] = resolutions
        self.results_df = self.results_df.sort_values(
            by=["resolution", "dataset_id"]
        )
        # update self.search_results to match the sorted DataFrame for future processing
        self.search_results = [
            EnhancedFile.fromdict(dict({k: v for k, v in row.items() if k != "_sa_instance_state"}))
            for _, row in self.results_df.iterrows()
        ]

    def search_message(self, search_state: str) -> None:
        """Display summary of file search."""
        console = Console()
        # create search table
        search_table = Table(
            title="Search Criteria",
            show_header=True,
            header_style="bold magenta",
        )
        search_table.add_column("Key", style="dim", width=20)
        search_table.add_column("Value", style="bold")
        for k, v in self.search_criteria.items():
            if k == "filter":
                for fk, fv in self.search_filter.items():
                    search_table.add_row(str(fk), str(fv))
            else:
                search_table.add_row(str(k), str(v))
        if search_state == "pre":
            # display search criteria
            console.print(
                Panel(search_table, title="[cyan]Searching", border_style="cyan")
            )
        if search_state == "post":
            if len(self.search_results) == self.limit:
                match_msg = " [orange1](limit of search reached)[/orange1]"
            else:
                match_msg = ""
            if len(self.search_results) > 1:
                file_str = "files"
            else:
                file_str = "file"
            msg = f"[green]Search completed.[/green] [bold]{len(self.search_results)}[/bold] {file_str}{match_msg} found matching criteria."  # noqa
            console.print(
                Panel(msg, title="[green]Search Results", border_style="green")
            )

    def get_top_n(self) -> pd.DataFrame | pd.Series:
        """
        Return all files associated with the top n groups,
        where groups are defined by ['institution_id', 'source_id', 'experiment_id'].
        """
        if self.results_df is None:
            raise ValueError("No results to select from. Run do_search() first.")

        top_n_to_use = self.top_n if self.top_n is not None else 3
        top_dataset_ids = self.results_df.drop_duplicates("dataset_id").head(
            top_n_to_use
        )["dataset_id"]
        return self.results_df[self.results_df["dataset_id"].isin(top_dataset_ids)]

    def clean_and_join_dict_vals(self, search_criteria: Optional[dict] = None):
        """Clean and join dictionary values to create a descriptive search ID for saving search results."""
        def clean_value(val):
            if isinstance(val, int):
                return str(val)
            if isinstance(val, str) and "," in val:
                # Split, strip, sort, join with no spaces
                items = sorted(map(str.strip, val.split(",")))
                return ",".join(items)
            if isinstance(val, str):
                return val.strip()
            return str(val)

        # Use provided search_criteria or self.search_criteria
        criteria_to_use = search_criteria if search_criteria is not None else self.search_criteria
        
        # clean all values, excluding the filter key (since this is a dictionary of strings)
        cleaned_str = [clean_value(v) for k, v in criteria_to_use.items() if k != "filter"]
        # order alphabetically in place to ensure consistent naming
        # cleaned_str.sort()
        # create search string in order: SEARCH_<project>_<table_id>_<experiment_id>_<variable>
        # Support variable_id (CLI facet name) as alias for variable
        project = criteria_to_use.get("project") or ""
        table_id = criteria_to_use.get("table_id") or ""
        experiment_id = criteria_to_use.get("experiment_id") or ""
        variable = criteria_to_use.get("variable") or criteria_to_use.get("variable_id") or ""
        return "SEARCH_" + str(project) + "_" + str(table_id) + "_" + str(experiment_id) + "_" + str(variable)
        # return "SEARCH_" + "_".join(cleaned_str).replace(" ", "")
    
    def _generate_subsearches(self) -> list[dict]:
        """
        Break down search criteria into individual subsearches.
        Creates one subsearch per combination of variable and experiment_id.
        Other criteria (table_id, frequency, etc.) are preserved in each subsearch.
        
        Returns:
            List of subsearch criteria dictionaries
        """
        subsearches = []
        
        # Get base criteria (everything except variable/variable_id and experiment_id and filter metadata)
        base_criteria = {k: v for k, v in self.search_criteria.items() 
                        if k not in ["variable", "variable_id", "experiment_id", "filter"]}
        
        # Parse variables and experiments (support variable_id as CLI alias)
        variables = []
        var_str = self.search_criteria.get("variable") or self.search_criteria.get("variable_id")
        if var_str is not None:
            if isinstance(var_str, str):
                variables = [v.strip() for v in var_str.split(",")]
            else:
                variables = [var_str]
        else:
            variables = [None]  # No variable specified
        
        experiments = []
        if "experiment_id" in self.search_criteria:
            exp_str = self.search_criteria["experiment_id"]
            if isinstance(exp_str, str):
                experiments = [e.strip() for e in exp_str.split(",")]
            else:
                experiments = [exp_str]
        else:
            experiments = [None]  # No experiment specified
        
        table_ids = []
        if "table_id" in self.search_criteria:
            table_str = self.search_criteria["table_id"]
            if isinstance(table_str, str):
                table_ids = [t.strip() for t in table_str.split(",")]
            else:
                table_ids = [table_str]
        else:
            table_ids = [None]  # No table specified
        # Use variable_id if that was in criteria (CLI/ESGF standard), else variable
        var_key = "variable_id" if "variable_id" in self.search_criteria else "variable"
        # Create subsearches for each variable-experiment-table combination
        for variable in variables:
            for experiment in experiments:
                for table_id in table_ids:
                    subsearch = base_criteria.copy()
                    if variable is not None:
                        subsearch[var_key] = variable
                    if experiment is not None:
                        subsearch["experiment_id"] = experiment
                    if table_id is not None:
                        subsearch["table_id"] = table_id
                    # Preserve filter settings
                    if "filter" in self.search_criteria:
                        subsearch["filter"] = self.search_criteria["filter"].copy()
                    subsearches.append(subsearch)
        
        return subsearches
    
    def _get_subsearch_cache_key(self, subsearch_criteria: dict) -> str:
        """Generate a cache key for a subsearch. Includes file vs dataset to avoid mixing caches."""
        base = self.clean_and_join_dict_vals(subsearch_criteria)
        return f"{base}_file" if self._file_search else f"{base}_dataset"
    
    def _load_subsearch_from_cache(self, subsearch_criteria: dict) -> Optional[pd.DataFrame]:
        """
        Load a specific subsearch from cache if available.
        Returns the DataFrame (even if empty) if cached, None if not cached.
        Empty DataFrames indicate a cached negative search (no results).
        """
        cache_key = self._get_subsearch_cache_key(subsearch_criteria)
        cache_file = self.search_results_dir / f"{cache_key}.csv"
        
        if cache_file.exists():
            try:
                # Check if file is empty (negative search marker)
                if cache_file.stat().st_size == 0:
                    # Empty file indicates a cached negative search
                    return pd.DataFrame()
                
                df = pd.read_csv(cache_file)
                if "_sa_instance_state" in df.columns:
                    df = df.drop(columns=["_sa_instance_state"])
                # Return DataFrame even if empty (indicates cached negative search)
                return df
            except pd.errors.EmptyDataError:
                # Empty CSV file - this is a cached negative search
                return pd.DataFrame()
            except Exception as e:
                log.warning(f"Could not load cache file {cache_file}: {e}")
                return None
        return None
    
    def _save_subsearch_to_cache(self, subsearch_criteria: dict, results_df: pd.DataFrame) -> None:
        """
        Save a subsearch result to cache (including empty results for negative searches).
        Empty DataFrames are saved as empty CSV files to mark negative searches.
        """
        try:
            self.search_results_dir.mkdir(parents=True, exist_ok=True)
            cache_key = self._get_subsearch_cache_key(subsearch_criteria)
            cache_file = self.search_results_dir / f"{cache_key}.csv"
            
            if not cache_file.exists():
                if results_df.empty:
                    # Save empty file as marker for negative search
                    cache_file.touch()
                    log.debug(f"Cached negative subsearch (no results): {cache_key}")
                else:
                    results_df.to_csv(cache_file, index=False)
                    log.debug(f"Cached subsearch ({len(results_df)} results): {cache_key}")
                log.debug(f"Cache file path: {cache_file}")
                # Verify file was created
                if cache_file.exists():
                    log.debug(f"Cache file verified: {cache_file.stat().st_size} bytes")
                else:
                    log.error(f"Cache file was not created at {cache_file}")
            else:
                result_count = "empty" if results_df.empty else f"{len(results_df)} results"
                log.debug(f"Subsearch already cached ({result_count}): {cache_key}")
                log.debug(f"Cache file path: {cache_file}")
        except (PermissionError, OSError) as e:
            log.warning(f"Could not save to cache (continuing without cache): {e}")
        except Exception as e:
            log.error(f"Error saving to cache: {e}")
            import traceback
            traceback.print_exc()

    def check_system_resources(self, output_dir=None):
        """Check system resources and warn if they might be insufficient. Used to adjust batch size based on system resources."""
        try:
            import psutil
        except ImportError:
            log.warning("psutil not available. Cannot check system resources.")
            return
        
        # check memory usage
        memory = psutil.virtual_memory()
        if memory.percent > 80:
            log.warning(f"High memory usage ({memory.percent:.1f}%). Consider reducing batch size.")
        
        # check available disk space if output_dir is provided
        if output_dir:
            try:
                disk = psutil.disk_usage(str(output_dir))
                free_gb = disk.free / (1024**3)
                if free_gb < 10:  # less than 10GB free
                    log.warning(f"Low disk space ({free_gb:.1f}GB free). Ensure sufficient space for downloads.")
            except (OSError, PermissionError):
                log.warning("Could not check disk space.")
        
        # check file descriptor limit (Unix systems)
        if hasattr(os, 'getrlimit'):
            try:
                soft, _ = os.getrlimit(os.RLIMIT_NOFILE)
                if soft < 1000:
                    log.warning(f"Low file descriptor limit ({soft}). May cause issues with many concurrent downloads.")
            except (OSError, AttributeError):
                pass

    def _get_adaptive_batch_size(self, requested_batch_size: int, total_files: int) -> int:
        """Adjust batch size based on system resources and total file count."""
        try:
            import psutil
        except ImportError:
            # if psutil not available, use conservative defaults
            if total_files > 1000:
                return min(requested_batch_size, 25)
            elif total_files > 500:
                return min(requested_batch_size, 40)
            return max(requested_batch_size, 5)
        
        # start with requested batch size
        batch_size = requested_batch_size
        
        # reduce batch size for very large file counts
        if total_files > 1000:
            batch_size = min(batch_size, 25)
        elif total_files > 500:
            batch_size = min(batch_size, 40)
        
        # check memory usage and reduce batch size if high
        try:
            memory = psutil.virtual_memory()
            if memory.percent > 70:
                batch_size = min(batch_size, 20)
            elif memory.percent > 50:
                batch_size = min(batch_size, 35)
        except Exception:
            pass  # if memory check fails, use the current batch size
        
        # return ensured minimum batch size
        return max(batch_size, 5)

    def save_searches(self) -> None:
        """Save the search results to a CSV file for future use and record keeping."""
        # check if search directory exists, if not create it
        self.search_results_dir.mkdir(parents=True, exist_ok=True)
        self.search_id = self.clean_and_join_dict_vals()    # create search id for filepath
        self.search_results_fp = self.search_results_dir / f"{self.search_id}.csv"
        if self.results_df is None:
            raise ValueError("No results to save. Run do_search() first.")

        if not self.search_results_fp.exists():
            self.results_df.to_csv(self.search_results_fp, index=False)
            log.info(f"Search results saved to {self.search_results_fp}")
        else:
            log.info(f"Search results already exist at {self.search_results_fp}. Not overwriting.")

    def load_search_results(self) -> pd.DataFrame:
        """Load search results from a CSV file."""
        search_fp = self.search_results_dir / f"{self.search_id}.csv"
        if search_fp.exists():
            self.results_df = pd.read_csv(search_fp)
            if "_sa_instance_state" in self.results_df.columns:
                self.results_df = self.results_df.drop(columns=["_sa_instance_state"])
            self.search_results_fp = search_fp
            self.search_results = [
                EnhancedFile.fromdict(dict({k: v for k, v in row.items() if k != "_sa_instance_state"}))
                for _, row in self.results_df.iterrows()
            ]
            return self.results_df
        else:
            raise FileNotFoundError(f"Search results file {search_fp} not found.")

    def run(self) -> list[EnhancedFile]:
        """
        Perform search, sort, and return top n results as EnhancedFile objects.
        Uses modular caching: breaks down search into subsearches (by variable/experiment),
        loads cached subsearches where available, performs new searches for missing ones,
        and combines all results.
        """
        if not self.search_criteria or not self.meta_criteria:
            self.load_config(config.search_criteria_fp)
        
        # Generate subsearches
        subsearches = self._generate_subsearches()
        log.info(f"Generated {len(subsearches)} subsearches")
        
        # Collect results from cache and new searches
        cached_results = []
        new_searches_needed = []
        
        # Check cache for each subsearch
        for subsearch in subsearches:
            cached_df = self._load_subsearch_from_cache(subsearch)
            if cached_df is not None:   # if cached result found (even if empty - indicates negative search was cached)
                # Cached result found (even if empty - indicates negative search was cached)
                if not cached_df.empty:
                    cached_results.append(cached_df)
                    log.debug(f"Loaded from cache ({len(cached_df)} results): {self._get_subsearch_cache_key(subsearch)}")
                else:
                    log.debug(f"Loaded negative search from cache (no results): {self._get_subsearch_cache_key(subsearch)}")
                # Don't add to new_searches_needed - this search is cached (even if empty)
            else:   # no searches found: add to new_searches_needed list
                new_searches_needed.append(subsearch)
        
        # Perform new searches for uncached subsearches
        if new_searches_needed:
            log.info(f"Performing {len(new_searches_needed)} new searches...")
            self.search_message("pre")
            
            api_instance = api.EsgpullAPI()
            for subsearch in new_searches_needed:
                try:
                    # Perform search for this subsearch
                    try:
                        # display the specific subsearch criteria
                        log.info(f"Performing subsearch: {self._get_subsearch_cache_key(subsearch)}")
                        results = api_instance.search(criteria=subsearch, file=self._file_search)
                    except ExceptionGroup as eg:
                        error_messages = []
                        for exc in eg.exceptions:
                            if isinstance(exc, httpx.HTTPStatusError):
                                status_code = exc.response.status_code
                                url = str(exc.request.url) if hasattr(exc, 'request') and exc.request else "unknown"
                                error_messages.append(
                                    f"ESGF server error {status_code} for {url}."
                                )
                            else:
                                error_messages.append(str(exc))
                        error_msg = "\n".join(error_messages)
                        log.warning(f"Error searching for {self._get_subsearch_cache_key(subsearch)}: {error_msg}")
                        continue
                    except httpx.HTTPStatusError as e:
                        status_code = e.response.status_code
                        url = str(e.request.url) if hasattr(e, 'request') and e.request else "unknown"
                        log.warning(f"ESGF server error {status_code} for {url} in subsearch {self._get_subsearch_cache_key(subsearch)}")
                        continue
                    except Exception as e:
                        log.warning(f"Error in subsearch {self._get_subsearch_cache_key(subsearch)}: {e}")
                        continue
                    
                    # Convert results to DataFrame
                    enhanced_results = []
                    # print(f"[SearchResults] Results type: {type(results)}")
                    
                    if results is None:
                        # log.warning(f"No results returned for subsearch {self._get_subsearch_cache_key(subsearch)}")
                        continue
                    
                    if not hasattr(results, '__iter__'):
                        # log.warning(f"Results is not iterable for subsearch {self._get_subsearch_cache_key(subsearch)}")
                        continue
                    
                    # Convert to list if it's a generator to avoid consuming it
                    if hasattr(results, '__next__') and not hasattr(results, '__len__'):
                        # print(f"[SearchResults] Results is a generator, converting to list...")
                        results = list(results)
                        log.debug(f"Converted to list with {len(results)} items")
                    
                    for result in results:
                        try:
                            if isinstance(result, dict):
                                enhanced_results.append(result)
                            else:
                                enhanced_file = EnhancedFile.fromdict(result)
                                enhanced_results.append(enhanced_file.asdict())
                        except Exception as e:
                            log.warning(f"Error processing result: {e}")
                            continue
                    
                    #                     # print(f"[SearchResults] Enhanced results count: {len(enhanced_results)}")
                    
                    # Always create DataFrame and save to cache (even if empty - this caches negative searches)
                    try:
                        subsearch_df = pd.DataFrame(enhanced_results)
                        if subsearch_df.empty:
                            log.info(f"No results found for subsearch: {self._get_subsearch_cache_key(subsearch)}")
                        else:
                            log.debug(f"DataFrame created with {len(subsearch_df)} rows")
                        
                        # Save to cache (including empty results for negative searches)
                        self._save_subsearch_to_cache(subsearch, subsearch_df)
                        log.debug(f"Saved to cache: {self._get_subsearch_cache_key(subsearch)}")
                        
                        # Only add non-empty results to cached_results
                        if not subsearch_df.empty:
                            cached_results.append(subsearch_df)
                    except Exception as e:
                        log.error(f"Error creating DataFrame or saving to cache: {e}")
                        import traceback
                        traceback.print_exc()
                        continue
                    
                except Exception as e:
                    log.warning(f"Failed to process subsearch {self._get_subsearch_cache_key(subsearch)}: {e}")
                    import traceback
                    traceback.print_exc()
                    continue
        
        # Combine all results
        if cached_results:
            self.results_df = pd.concat(cached_results, ignore_index=True)
            # Remove duplicates: file_id for file search, dataset_id for dataset search
            if "file_id" in self.results_df.columns:
                self.results_df = self.results_df.drop_duplicates(subset=["file_id"], keep="first")
            elif "dataset_id" in self.results_df.columns:
                self.results_df = self.results_df.drop_duplicates(subset=["dataset_id"], keep="first")
        else:
            self.results_df = pd.DataFrame()
        
        if self.results_df is not None and not self.results_df.empty:
            self.sort_results_by_metadata()
            self.search_message("post")
        else:
            log.info("No results found for given criteria.")
            return []
        
        # always get top_n from the current results_df
        top_n_df = self.get_top_n() if self.top_n else self.results_df
        # limit results to return
        if self.limit and top_n_df is not None:
            top_n_df = top_n_df.head(self.limit)

        return [EnhancedFile.fromdict(dict({k: v for k, v in row.items() if k != "_sa_instance_state"})) for _, row in top_n_df.iterrows()]

    def get_enhanced_metadata_summary(self) -> dict:
        """Get a summary of all available enhanced metadata fields."""
        if self.results_df is None or self.results_df.empty:
            return {}
        
        # Get all metadata fields (excluding base file fields)
        base_fields = {'file_id', 'dataset_id', 'master_id', 'url', 'version', 'filename', 'local_path', 'data_node', 'checksum', 'checksum_type', 'size', 'status'}
        metadata_fields = [col for col in self.results_df.columns if col not in base_fields]
        
        summary = {}
        for field in metadata_fields:
            unique_values = self.results_df[field].dropna().unique()
            if len(unique_values) > 0:
                summary[field] = {
                    'count': len(unique_values),
                    'values': list(unique_values[:5]) if len(unique_values) > 5 else list(unique_values),
                    'has_more': len(unique_values) > 5
                }
        
        return summary

    def display_enhanced_metadata_info(self) -> None:
        """Display information about available enhanced metadata."""
        summary = self.get_enhanced_metadata_summary()
        
        if not summary:
            log.info("No enhanced metadata available.")
            return
        
        log.info(f"Enhanced Metadata Summary: {len(summary)} fields")
        for field, info in summary.items():
            vals = ', '.join(map(str, info['values']))
            suffix = "..." if info['has_more'] else ""
            log.info(f"  {field}: {info['count']} unique values - {vals}{suffix}")

    def get_experiments_by_source(self, variable: Optional[str] = None) -> pd.DataFrame:
        """Delegate to :mod:`search_analysis`. See :func:`search_analysis.get_experiments_by_source`."""
        from esgpull.esgpullplus import search_analysis
        return search_analysis.get_experiments_by_source(self.results_df, variable=variable)

    def summarize_symmetrical_datasets(self) -> pd.DataFrame:
        """Delegate to :mod:`search_analysis`. See :func:`search_analysis.summarize_symmetrical_datasets`."""
        from esgpull.esgpullplus import search_analysis
        return search_analysis.summarize_symmetrical_datasets(self.results_df)

    def analyze_source_availability(
        self,
        historical_experiment: str = "historical",
        ssp_pattern: str = "ssp",
        require_both: bool = True,
    ) -> pd.DataFrame:
        """Delegate to :mod:`search_analysis`. See :func:`search_analysis.analyze_source_availability`."""
        from esgpull.esgpullplus import search_analysis
        return search_analysis.analyze_source_availability(
            self.results_df,
            historical_experiment=historical_experiment,
            ssp_pattern=ssp_pattern,
            require_both=require_both,
        )

    def visualize_source_availability(self, analysis_df=None, **kwargs) -> dict:
        """Delegate to :mod:`search_analysis`. See :func:`search_analysis.visualize_source_availability`."""
        from esgpull.esgpullplus import search_analysis
        if analysis_df is None:
            analysis_df = self.analyze_source_availability(**{
                k: v for k, v in kwargs.items()
                if k in ("historical_experiment", "ssp_pattern", "require_both")
            })
        return search_analysis.visualize_source_availability(analysis_df, **{
            k: v for k, v in kwargs.items()
            if k in ("output_dir", "show_plots", "save_plots")
        })

    def analyze_and_visualize_sources(self, **kwargs) -> tuple[pd.DataFrame, dict]:
        """Delegate to :mod:`search_analysis`. See :func:`search_analysis.analyze_and_visualize`."""
        from esgpull.esgpullplus import search_analysis
        return search_analysis.analyze_and_visualize(self.results_df, **kwargs)
