# general
import logging
import os
from pathlib import Path
from typing import Any, Optional

import pandas as pd
from rich.console import Console
from rich.panel import Panel
import httpx
import traceback

# rich
from rich.table import Table

# custom
from esgpull.esgpullplus import api, fileops, config, utils, search_analysis
from esgpull.esgpullplus.enhanced_file import EnhancedFile


log = logging.getLogger(__name__)

# Facets that narrow a search; included in cache keys and applied when loading broad caches.
_RESTRICTING_FACETS = (
    "institution_id",
    "source_id",
    "member_id",
    "grid_label",
    "variant_label",
)

# Facets that must be set in search.yaml before running downloads (at least one alias each).
_LIMITING_SEARCH_FACETS: tuple[tuple[str, ...], ...] = (
    ("variable", "variable_id"),
    ("experiment_id",),
)
_LIMITING_SEARCH_FACET_LABELS: dict[tuple[str, ...], str] = {
    ("variable", "variable_id"): "variable",
    ("experiment_id",): "experiment_id",
}


def normalize_search_criteria(criteria: dict) -> dict:
    """Normalize YAML/CLI aliases (e.g. variant_id → member_id)."""
    normalized = dict(criteria)
    if normalized.get("variant_id") and not normalized.get("member_id"):
        normalized["member_id"] = normalized.pop("variant_id")
    else:
        normalized.pop("variant_id", None)
    if normalized.get("variant_label") and not normalized.get("member_id"):
        normalized["member_id"] = normalized.pop("variant_label")
    else:
        normalized.pop("variant_label", None)
    return normalized


def prepare_esgf_search_criteria(criteria: dict) -> dict:
    """
    Map YAML search criteria to ESGF/metagrid facets.

    CMIP6 searches on the federated index use ``variable_id`` and
    ``variant_label`` (not ``variable`` / ``member_id``). ``member_id`` is kept
    in subsearch criteria for local filtering only.
    """
    out = {
        k: v
        for k, v in criteria.items()
        if k not in ("filter", "member_id", "variant_id")
    }
    if out.get("variable") and not out.get("variable_id"):
        out["variable_id"] = out.pop("variable")
    elif "variable" in out and "variable_id" in out:
        out.pop("variable", None)

    member = criteria.get("member_id")
    if member and not out.get("variant_label"):
        out["variant_label"] = member
    return out


def _broad_subsearch_criteria(subsearch: dict) -> dict:
    """Drop locally-filtered facets for a wider ESGF query."""
    return {
        k: v
        for k, v in subsearch.items()
        if k not in _RESTRICTING_FACETS and k != "filter"
    }


def _facet_value_is_set(value) -> bool:
    if value is None:
        return False
    if isinstance(value, str):
        return bool(value.strip())
    return bool(value)


def missing_limiting_search_facets(search_criteria: dict) -> list[str]:
    """
    Return limiting search facets that are missing or empty.

    These facets (``variable``, ``experiment_id``) are required for meaningful
    ESGF searches. Omitting them produces overly broad queries that often fail
    silently with no downloadable results.
    """
    criteria = normalize_search_criteria(search_criteria)
    missing: list[str] = []
    for facet_keys in _LIMITING_SEARCH_FACETS:
        if not any(_facet_value_is_set(criteria.get(key)) for key in facet_keys):
            missing.append(_LIMITING_SEARCH_FACET_LABELS[facet_keys])
    return missing


def format_missing_limiting_facets_message(missing: list[str]) -> str:
    joined = ", ".join(missing)
    return (
        f"Missing required search criteria: {joined}. "
        f"Set {joined} in search.yaml (comma-separated values are supported). "
        "Searching without these facets is unsupported and usually returns no results."
    )


def validate_limiting_search_criteria(search_criteria: dict) -> None:
    """Raise ValueError if any limiting search facet is missing or empty."""
    missing = missing_limiting_search_facets(search_criteria)
    if missing:
        raise ValueError(format_missing_limiting_facets_message(missing))


def _parse_facet_values(value) -> list[str]:
    if value is None:
        return []
    if isinstance(value, str):
        return [v.strip() for v in value.split(",") if v.strip()]
    return [str(value).strip()]


def _grid_label_from_dataset_id(dataset_id: str) -> Optional[str]:
    """Parse CMIP6 DRS grid_label from a dataset_id (… .Omon.var.grid.vYYYYMMDD)."""
    parts = str(dataset_id).split(".")
    if len(parts) >= 2 and parts[-1].startswith("v"):
        return parts[-2]
    return None


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
            self.search_criteria = normalize_search_criteria(search_criteria)
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
        self.search_criteria = normalize_search_criteria(
            config.get("search_criteria", {})
        )
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
        Return all files from the top n datasets per variable (and experiment).

        ``top_n`` is applied separately within each ``(variable_id, experiment_id)``
        group so multi-variable searches (e.g. tos, ph, talk) each retain their own
        highest-resolution datasets instead of competing globally.
        """
        if self.results_df is None:
            raise ValueError("No results to select from. Run do_search() first.")

        top_n_to_use = self.top_n if self.top_n is not None else 3
        df = self.results_df

        group_cols: list[str] = []
        if "variable_id" in df.columns:
            group_cols.append("variable_id")
        elif "variable" in df.columns:
            group_cols.append("variable")
        if "experiment_id" in df.columns:
            group_cols.append("experiment_id")

        if not group_cols:
            top_dataset_ids = df.drop_duplicates("dataset_id").head(top_n_to_use)[
                "dataset_id"
            ]
            return df[df["dataset_id"].isin(top_dataset_ids)]

        parts: list[pd.DataFrame] = []
        for _, group in df.groupby(group_cols, sort=False):
            top_dataset_ids = group.drop_duplicates("dataset_id").head(top_n_to_use)[
                "dataset_id"
            ]
            parts.append(group[group["dataset_id"].isin(top_dataset_ids)])

        if not parts:
            return df.iloc[0:0]
        return pd.concat(parts, ignore_index=True)

    def _apply_year_filter_df(self, df: pd.DataFrame) -> pd.DataFrame:
        """Subset results by meta_criteria year ranges (before top_n/limit)."""
        year_filter = api.parse_year_filter_config(self.meta_criteria or {})
        if not year_filter.is_active or df.empty:
            return df
        records = df.to_dict(orient="records")
        filtered = api.filter_files_by_year_config(records, year_filter)
        if not filtered:
            return df.iloc[0:0]
        return pd.DataFrame(filtered)

    def _sync_search_results_from_df(self) -> None:
        """Refresh ``search_results`` from the current ``results_df``."""
        if self.results_df is None or self.results_df.empty:
            self.search_results = []
            return
        self.search_results = [
            EnhancedFile.fromdict(
                {k: v for k, v in row.items() if k != "_sa_instance_state"}
            )
            for _, row in self.results_df.iterrows()
        ]

    def _finalize_search_results(self) -> list[EnhancedFile]:
        """Sort, apply time subsetting, then top_n/limit (in that order)."""
        if self.results_df is None or self.results_df.empty:
            log.info("No results found for given criteria.")
            return []

        self.sort_results_by_metadata()
        original_count = len(self.results_df)
        self.results_df = self._apply_year_filter_df(self.results_df)
        removed = original_count - len(self.results_df)
        if removed > 0:
            year_filter = api.parse_year_filter_config(self.meta_criteria or {})
            msg = (
                f"Time filter ({year_filter.describe()}): removed {removed} "
                f"file{'s' if removed != 1 else ''} outside range "
                f"({len(self.results_df)} remaining)"
            )
            log.info(msg)
            Console().print(f"[cyan]📅 {msg}[/cyan]")
        self._sync_search_results_from_df()
        self.search_message("post")

        top_n_df = self.get_top_n() if self.top_n else self.results_df
        if self.limit and top_n_df is not None:
            top_n_df = top_n_df.head(self.limit)

        return [
            EnhancedFile.fromdict(
                {k: v for k, v in row.items() if k != "_sa_instance_state"}
            )
            for _, row in top_n_df.iterrows()
        ]

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
        parts = [project, table_id, experiment_id, variable]
        for facet in _RESTRICTING_FACETS:
            value = criteria_to_use.get(facet)
            if value:
                parts.append(clean_value(value))
        return "SEARCH_" + "_".join(str(p) for p in parts if p)

    def _apply_facet_filters(
        self, df: pd.DataFrame, criteria: dict
    ) -> pd.DataFrame:
        """Filter cached/unfiltered results to match subsearch facets."""
        if df is None or df.empty:
            return df

        out = df
        skip_keys = {"filter"}
        for key, raw in criteria.items():
            if key in skip_keys or raw is None:
                continue
            values = _parse_facet_values(raw)
            if not values:
                continue
            if key == "member_id":
                mask = None
                if "member_id" in out.columns:
                    mask = out["member_id"].astype(str).isin(values)
                if "variant_label" in out.columns:
                    variant_mask = out["variant_label"].astype(str).isin(values)
                    mask = (
                        variant_mask
                        if mask is None
                        else (mask | variant_mask)
                    )
                if mask is not None:
                    out = out[mask]
                continue
            if key == "grid_label":
                if "grid_label" in out.columns:
                    out = out[out["grid_label"].astype(str).isin(values)]
                elif "dataset_id" in out.columns:
                    out = out[
                        out["dataset_id"]
                        .map(_grid_label_from_dataset_id)
                        .astype(str)
                        .isin(values)
                    ]
                continue
            col = key
            if col not in out.columns:
                if key == "variable" and "variable_id" in out.columns:
                    col = "variable_id"
                elif key == "variable_id" and "variable" in out.columns:
                    col = "variable"
                else:
                    continue
            out = out[out[col].astype(str).isin(values)]
        return out

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
        # Create subsearches for each variable-experiment-table combination
        for variable in variables:
            for experiment in experiments:
                for table_id in table_ids:
                    subsearch = base_criteria.copy()
                    subsearch.pop("variable", None)
                    subsearch.pop("variable_id", None)
                    if variable is not None:
                        subsearch["variable_id"] = variable
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

    def _subsearch_cache_path(self, subsearch_criteria: dict, kind: str) -> Path:
        base = self.clean_and_join_dict_vals(subsearch_criteria)
        return self.search_results_dir / f"{base}_{kind}.csv"
    
    def _read_cache_csv(self, cache_file: Path) -> Optional[pd.DataFrame]:
        """Read a cache CSV; empty file means cached negative search."""
        try:
            if cache_file.stat().st_size == 0:
                return pd.DataFrame()
            df = pd.read_csv(cache_file)
            if "_sa_instance_state" in df.columns:
                df = df.drop(columns=["_sa_instance_state"])
            return df
        except pd.errors.EmptyDataError:
            return pd.DataFrame()
        except Exception as e:
            log.warning(f"Could not load cache file {cache_file}: {e}")
            return None

    def _cache_file_is_negative(self, cache_file: Path) -> bool:
        """True when a cache file exists but marks a failed/empty subsearch."""
        return cache_file.exists() and cache_file.stat().st_size == 0

    def _respect_negative_cache(self) -> bool:
        return bool(self.meta_criteria.get("cache_negative_searches", False))

    def _load_broad_subsearch_from_cache(
        self, subsearch_criteria: dict, cache_key: str
    ) -> Optional[pd.DataFrame]:
        """Reuse a broad (unrestricted) cached subsearch and apply local facet filters."""
        has_restrictions = any(
            subsearch_criteria.get(facet) for facet in _RESTRICTING_FACETS
        )
        if not has_restrictions:
            return None

        broad_criteria = _broad_subsearch_criteria(subsearch_criteria)
        preferred_kind = "file" if self._file_search else "dataset"
        for kind in (preferred_kind, "dataset" if preferred_kind == "file" else "file"):
            broad_file = self._subsearch_cache_path(broad_criteria, kind)
            if not broad_file.exists():
                continue
            if self._cache_file_is_negative(broad_file) and not self._respect_negative_cache():
                continue
            broad_df = self._read_cache_csv(broad_file)
            if broad_df is None:
                continue
            if broad_df.empty:
                return broad_df
            filtered = self._apply_facet_filters(broad_df, subsearch_criteria)
            log.debug(
                "Loaded broad %s cache %s, filtered to %s rows for %s",
                kind,
                broad_file.name,
                len(filtered),
                cache_key,
            )
            return filtered
        return None

    def _load_subsearch_from_cache(self, subsearch_criteria: dict) -> Optional[pd.DataFrame]:
        """
        Load a specific subsearch from cache if available.
        Returns the DataFrame (even if empty) if cached, None if not cached.
        Empty DataFrames indicate a cached negative search (no results).
        """
        cache_key = self._get_subsearch_cache_key(subsearch_criteria)
        cache_file = self.search_results_dir / f"{cache_key}.csv"
        stale_negative = False

        if cache_file.exists():
            if self._cache_file_is_negative(cache_file) and not self._respect_negative_cache():
                log.info(
                    "Ignoring stale empty (negative) cache; will reuse broad cache or re-search: %s",
                    cache_key,
                )
                stale_negative = True
            else:
                return self._read_cache_csv(cache_file)

        broad_cached = self._load_broad_subsearch_from_cache(subsearch_criteria, cache_key)
        if broad_cached is not None:
            return broad_cached

        if stale_negative or not cache_file.exists():
            return None
        return self._read_cache_csv(cache_file)

    def _should_write_cache(self, cache_file: Path, results_df: pd.DataFrame) -> bool:
        """Decide whether to write cache contents to disk."""
        if not cache_file.exists():
            return True
        if self._cache_file_is_negative(cache_file):
            # Replace stale negative markers with fresh search results.
            return True
        if results_df.empty:
            return False
        return False

    def _write_cache_file(
        self, cache_file: Path, results_df: pd.DataFrame, cache_key: str
    ) -> None:
        if results_df.empty:
            cache_file.write_bytes(b"")
            log.debug(f"Cached negative subsearch (no results): {cache_key}")
        else:
            results_df.to_csv(cache_file, index=False)
            log.info(
                f"Cached subsearch ({len(results_df)} results): {cache_key} -> {cache_file.name}"
            )

    def _save_subsearch_to_cache(
        self,
        subsearch_criteria: dict,
        results_df: pd.DataFrame,
        kind: str | None = None,
    ) -> None:
        """
        Save a subsearch result to cache (including empty results for negative searches).
        Empty DataFrames are saved as empty CSV files to mark negative searches.

        Stale negative cache files (0 bytes) are overwritten when a later search
        succeeds, matching the download path that ignores empty caches and re-queries ESGF.
        """
        try:
            self.search_results_dir.mkdir(parents=True, exist_ok=True)
            cache_key = self._get_subsearch_cache_key(subsearch_criteria)
            if kind is not None:
                cache_file = self._subsearch_cache_path(subsearch_criteria, kind)
            else:
                cache_file = self.search_results_dir / f"{cache_key}.csv"

            if not self._should_write_cache(cache_file, results_df):
                result_count = "empty" if results_df.empty else f"{len(results_df)} results"
                log.debug(f"Subsearch already cached ({result_count}): {cache_key}")
                return

            if self._cache_file_is_negative(cache_file) and not results_df.empty:
                log.info(
                    "Refreshing stale negative cache with %s results: %s",
                    len(results_df),
                    cache_file.name,
                )

            self._write_cache_file(cache_file, results_df, cache_key)
        except (PermissionError, OSError) as e:
            log.warning(f"Could not save to cache (continuing without cache): {e}")
        except Exception as e:
            log.error(f"Error saving to cache: {e}")
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

    def _needs_file_expansion(self, df: pd.DataFrame) -> bool:
        if df is None or df.empty or not self._file_search:
            return False
        if "filename" in df.columns or "url" in df.columns:
            return False
        return "dataset_id" in df.columns

    def _expand_dataset_results_to_files(
        self,
        api_instance: Any,
        dataset_results: list[dict],
    ) -> list[dict]:
        """Resolve file-level records (URLs) from dataset search hits."""
        files: list[dict] = []
        seen: set[str] = set()
        for record in dataset_results:
            dataset_id = record.get("dataset_id")
            if not dataset_id or dataset_id in seen:
                continue
            seen.add(dataset_id)
            try:
                file_hits = api_instance.search(
                    {"dataset_id": str(dataset_id)}, file=True
                )
            except Exception as exc:
                log.warning("File expansion failed for %s: %s", dataset_id, exc)
                continue
            for hit in file_hits:
                files.append(hit if isinstance(hit, dict) else hit.asdict())
        return files

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
                if not cached_df.empty:
                    # Broad-cache fallback is pre-filtered; narrow cache is not.
                    if (self.search_results_dir / f"{self._get_subsearch_cache_key(subsearch)}.csv").exists():
                        cached_df = self._apply_facet_filters(cached_df, subsearch)
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
                        esgf_criteria = prepare_esgf_search_criteria(subsearch)
                        results = api_instance.search(
                            criteria=esgf_criteria, file=self._file_search
                        )
                        if not results and any(
                            subsearch.get(facet) for facet in _RESTRICTING_FACETS
                        ):
                            broad_criteria = prepare_esgf_search_criteria(
                                _broad_subsearch_criteria(subsearch)
                            )
                            log.info(
                                "No results with restricting facets; retrying broad ESGF search: %s",
                                self._get_subsearch_cache_key(
                                    _broad_subsearch_criteria(subsearch)
                                ),
                            )
                            results = api_instance.search(
                                criteria=broad_criteria, file=self._file_search
                            )
                            if results:
                                self._save_subsearch_to_cache(
                                    _broad_subsearch_criteria(subsearch),
                                    pd.DataFrame(
                                        [
                                            r
                                            if isinstance(r, dict)
                                            else EnhancedFile.fromdict(r).asdict()
                                            for r in results
                                        ]
                                    ),
                                )
                        if not results and self._file_search:
                            dataset_results = api_instance.search(
                                criteria=esgf_criteria, file=False
                            )
                            if not dataset_results:
                                dataset_results = api_instance.search(
                                    criteria=prepare_esgf_search_criteria(
                                        _broad_subsearch_criteria(subsearch)
                                    ),
                                    file=False,
                                )
                            if dataset_results:
                                results = self._expand_dataset_results_to_files(
                                    api_instance, dataset_results
                                )
                                broad = _broad_subsearch_criteria(subsearch)
                                self._save_subsearch_to_cache(
                                    broad,
                                    pd.DataFrame(dataset_results),
                                    kind="dataset",
                                )
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
                            subsearch_df = self._apply_facet_filters(
                                subsearch_df, subsearch
                            )
                            if not subsearch_df.empty:
                                cached_results.append(subsearch_df)
                    except Exception as e:
                        log.error(f"Error creating DataFrame or saving to cache: {e}")
                        traceback.print_exc()
                        continue
                    
                except Exception as e:
                    log.warning(f"Failed to process subsearch {self._get_subsearch_cache_key(subsearch)}: {e}")
                    traceback.print_exc()
                    continue
        
        # Combine all results
        if cached_results:
            self.results_df = pd.concat(cached_results, ignore_index=True)
            if self._needs_file_expansion(self.results_df):
                api_instance = api.EsgpullAPI()
                expanded = self._expand_dataset_results_to_files(
                    api_instance,
                    self.results_df.to_dict(orient="records"),
                )
                if expanded:
                    self.results_df = pd.DataFrame(expanded)
            # Remove duplicates: file_id for file search, dataset_id for dataset search
            if "file_id" in self.results_df.columns:
                self.results_df = self.results_df.drop_duplicates(subset=["file_id"], keep="first")
            elif "dataset_id" in self.results_df.columns:
                self.results_df = self.results_df.drop_duplicates(subset=["dataset_id"], keep="first")
        else:
            self.results_df = pd.DataFrame()

        return self._finalize_search_results()

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
        return search_analysis.get_experiments_by_source(self.results_df, variable=variable)

    def summarize_symmetrical_datasets(self) -> pd.DataFrame:
        """Delegate to :mod:`search_analysis`. See :func:`search_analysis.summarize_symmetrical_datasets`."""
        return search_analysis.summarize_symmetrical_datasets(self.results_df)

    def analyze_source_availability(
        self,
        historical_experiment: str = "historical",
        ssp_pattern: str = "ssp",
        require_both: bool = True,
    ) -> pd.DataFrame:
        """Delegate to :mod:`search_analysis`. See :func:`search_analysis.analyze_source_availability`."""
        return search_analysis.analyze_source_availability(
            self.results_df,
            historical_experiment=historical_experiment,
            ssp_pattern=ssp_pattern,
            require_both=require_both,
        )

    def visualize_source_availability(self, analysis_df=None, **kwargs) -> dict:
        """Delegate to :mod:`search_analysis`. See :func:`search_analysis.visualize_source_availability`."""
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
        return search_analysis.analyze_and_visualize(self.results_df, **kwargs)
