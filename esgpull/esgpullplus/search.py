# general
import os
import re
from typing import Optional

import pandas as pd
from rich.console import Console
from rich.panel import Panel

# rich
from rich.table import Table

# custom
from esgpull.esgpullplus import api, fileops, config
from esgpull.esgpullplus.enhanced_file import EnhancedFile
    

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
    ):
        self.search_criteria = search_criteria if search_criteria else {}
        self.meta_criteria = meta_criteria if meta_criteria else {}
        self.search_filter = self.search_criteria.get("filter", {})
        self.top_n = self.search_filter.get("top_n", None)
        self.limit = self.search_filter.get("limit", 4)   # limit results to return, good for debugging
        self.search_results = []  # List to hold EnhancedFile objects from search
        self.results_df = None  # DataFrame to hold results for further processing
        self.results_df_top = None  # DataFrame for top N results from search
        self.fs = api.EsgpullAPI().esg.fs  # File system from Esgpull API
        self.search_results_dir = self.fs.paths.data / "search_results"

    def load_config(self, config_path: str) -> None:
        """Load search criteria and metadata from a YAML configuration file."""
        config = fileops.read_yaml(config_path)
        self.search_criteria = config.get("search_criteria", {})
        self.meta_criteria = config.get("meta_criteria", {})
        self.search_filter = self.search_criteria.get("filter", {})
        self.top_n = self.search_filter.get("top_n", None)  # get top n of grouped data ie. first n models from ensemble
        self.limit = self.search_filter.get("limit", 4)   # good for debugging

    def do_search(self) -> None:
        """Perform a search using the provided criteria and populate results with enhanced metadata."""
        api_instance = api.EsgpullAPI()
        
        # Use enhanced search to get all available metadata
        results = api_instance.search(criteria=self.search_criteria)
        
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
            print("[SearchResults] No results found for given criteria.")

    def sort_results_by_metadata(self) -> None:
        """Sort a list of EnhancedFile objects by institution_id, source_id, experiment_id, member_id."""
        if self.results_df is None or self.results_df.empty:
            print("[SearchResults] No results to sort.")
            return
        # convert resolutions to float for sorting
        resolutions = self.results_df.apply(
            lambda f: self.calc_resolution(f.nominal_resolution), axis=1
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

    def calc_resolution(self, res) -> float:
        """
        Extract nominal resolution from file.nominal_resolution and return in degrees.
        Supports 'xx km', 'x x degree', or 'x degree'. Returns large value if unknown.
        Handles both string and numeric input.
        """
        if isinstance(res, (float, int)):
            return float(res)
        if not res:
            return 9999.0
        res = str(res).lower().replace(" ", "")
        if m := re.match(r"([\d.]+)km", res):
            return float(m.group(1)) / 111.0
        if m := re.match(r"([\d.]+)x([\d.]+)degree", res):
            return (float(m.group(1)) + float(m.group(2))) / 2.0
        if m := re.match(r"([\d.]+)degree", res):
            return float(m.group(1))
        return 9999.0

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
                Panel(search_table, title="[cyan]Starting Search", border_style="cyan")
            )
        if search_state == "post":
            if len(self.search_results) == self.limit:
                match_msg = " [orange1](limit of search reached)[/orange1]"
            else:
                match_msg = ""
            msg = f"[green]Search completed.[/green] [bold]{len(self.search_results)}[/bold] files{match_msg} found matching criteria."  # noqa
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

    def clean_and_join_dict_vals(self):
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

        # clean all values, excluding the filter key (since this is a dictionary of strings)
        cleaned_str = [clean_value(v) for k, v in self.search_criteria.items() if k != "filter"]
        # order alphabetically in place to ensure consistent naming
        cleaned_str.sort()
        return "SEARCH_" + "_".join(cleaned_str).replace(" ", "")

    def check_system_resources(self, output_dir=None):
        """Check system resources and warn if they might be insufficient. Used to adjust batch size based on system resources."""
        try:
            import psutil
        except ImportError:
            print("[yellow]Warning: psutil not available. Cannot check system resources.[/yellow]")
            return
        
        # check memory usage
        memory = psutil.virtual_memory()
        if memory.percent > 80:
            print(f"[yellow]Warning: High memory usage ({memory.percent:.1f}%). Consider reducing batch size.[/yellow]")
        
        # check available disk space if output_dir is provided
        if output_dir:
            try:
                disk = psutil.disk_usage(str(output_dir))
                free_gb = disk.free / (1024**3)
                if free_gb < 10:  # less than 10GB free
                    print(f"[yellow]Warning: Low disk space ({free_gb:.1f}GB free). Ensure sufficient space for downloads.[/yellow]")
            except (OSError, PermissionError):
                print("[yellow]Warning: Could not check disk space.[/yellow]")
        
        # check file descriptor limit (Unix systems)
        if hasattr(os, 'getrlimit'):
            try:
                soft, _ = os.getrlimit(os.RLIMIT_NOFILE)
                if soft < 1000:
                    print(f"[yellow]Warning: Low file descriptor limit ({soft}). May cause issues with many concurrent downloads.[/yellow]")
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
            print(f"Search results saved to {self.search_results_fp}")
        else:
            print(
                f"Search results already exist at {self.search_results_fp}. Not overwriting."
            )

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
        """Perform search, sort, and return top n results as EnhancedFile objects. Loads from cache if available, else performs search and saves."""
        if not self.search_criteria or not self.meta_criteria:
            self.load_config(config.search_criteria_fp)
        # try to load from cache if available
        try:
            self.search_id = self.clean_and_join_dict_vals()
            self.load_search_results()
            print(f"Loaded search results from cache: {self.search_results_fp}")
            self.search_message("post")
        # if search results file not found, perform search and save
        except FileNotFoundError:
            self.search_message("pre")
            self.do_search()
            if self.results_df is not None and self.results_df.empty:
                print("[SearchResults] No results found for given criteria.")
                return []
            self.search_message("post")
            self.sort_results_by_metadata()
            self.save_searches()
        # always get top_n from the current results_df
        top_n_df = self.get_top_n() if self.top_n else self.results_df
        # limit results to return
        if self.limit and top_n_df is not None:
            top_n_df = top_n_df.head(self.limit)
        return top_n_df

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
            print("[SearchResults] No enhanced metadata available.")
            return
        
        print(f"\n[SearchResults] Enhanced Metadata Summary:")
        print(f"Total metadata fields: {len(summary)}")
        
        for field, info in summary.items():
            print(f"  {field}: {info['count']} unique values")
            if info['has_more']:
                print(f"    Examples: {', '.join(map(str, info['values']))}...")
            else:
                print(f"    Values: {', '.join(map(str, info['values']))}")
