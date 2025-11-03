#!/usr/bin/env python3
"""
CMIP6 Data Loader for regridded climate model output.

This module provides functionality to:
1. Catalog/index downloaded CMIP6 files
2. Provide overview of available data (variables, scenarios, years, etc.)
3. Load specific variables/scenarios/ensembles into xarray datasets
4. Support future ML training workflows with ensemble loading
"""

import re
from pathlib import Path
from typing import Optional, Dict, List, Set, Tuple, Union
from dataclasses import dataclass, field
from collections import defaultdict
from datetime import datetime

import xarray as xa
import pandas as pd
from rich.console import Console
from rich.table import Table
from rich import print as rich_print

from esgpull.esgpullplus import utils as esgpull_utils

# ========
# TODO
# ========

@dataclass
class FileMetadata:
    """Metadata for a single CMIP6 file."""
    path: Path
    mip: str  # e.g., "CMIP", "ScenarioMIP"
    institute: str  # e.g., "AWI", "CCCma"
    model: str  # e.g., "AWI-CM-1-1-MR", "CanESM5"
    experiment: str  # e.g., "historical", "ssp585"
    variant: str  # e.g., "r1i1p1f1"
    frequency: str  # e.g., "Omon"
    variable: str  # e.g., "tos", "no3"
    grid: str  # e.g., "gn"
    version: str  # e.g., "v20181218"
    filename: str
    is_regridded: bool = False
    is_top_level: bool = False
    time_start: Optional[int] = None  # e.g., 185001
    time_end: Optional[int] = None  # e.g., 185012
    year_start: Optional[int] = None  # e.g., 1850
    year_end: Optional[int] = None  # e.g., 1850
    
    def get_source_key(self) -> str:
        """Get a unique identifier for the data source (institute/model/variant combination)."""
        return f"{self.institute}/{self.model}/{self.variant}"
    
    def get_source_id(self) -> Dict[str, str]:
        """Get source information as a dictionary."""
        return {
            'institute': self.institute,
            'model': self.model,
            'variant': self.variant,
            'version': self.version,
        }
    
    @classmethod
    def from_path(cls, path: Path) -> Optional['FileMetadata']:
        """Parse file path and create metadata."""
        # Example: /maps/rt582/data/CMIP6/CMIP/AWI/AWI-CM-1-1-MR/historical/r1i1p1f1/Omon/tos/gn/v20181218/tos_Omon_AWI-CM-1-1-MR_historical_r1i1p1f1_gn_185001-185012.nc
        parts = path.parts
        
        if 'CMIP6' not in parts:
            return None
        
        cmip6_idx = parts.index('CMIP6')
        if len(parts) < cmip6_idx + 11:  # Need at least 11 parts after CMIP6
            return None
        
        try:
            mip = parts[cmip6_idx + 1]
            institute = parts[cmip6_idx + 2]
            model = parts[cmip6_idx + 3]
            experiment = parts[cmip6_idx + 4]
            variant = parts[cmip6_idx + 5]
            frequency = parts[cmip6_idx + 6]
            variable = parts[cmip6_idx + 7]
            grid = parts[cmip6_idx + 8]
            version = parts[cmip6_idx + 9]
            filename = parts[cmip6_idx + 10]
            
            # Check if regridded or top_level
            is_regridded = '_regridded' in filename
            is_top_level = '_top_level' in filename
            
            # Extract time range from filename
            # Pattern: variable_frequency_model_experiment_variant_grid_YYYYMM-YYYYMM_suffix.nc
            time_match = re.search(r'_(\d{6})-(\d{6})', filename)
            time_start = None
            time_end = None
            year_start = None
            year_end = None
            
            if time_match:
                time_start = int(time_match.group(1))  # e.g., 185001
                time_end = int(time_match.group(2))  # e.g., 185012
                year_start = time_start // 100  # e.g., 1850
                year_end = time_end // 100  # e.g., 1850
            
            return cls(
                path=path,
                mip=mip,
                institute=institute,
                model=model,
                experiment=experiment,
                variant=variant,
                frequency=frequency,
                variable=variable,
                grid=grid,
                version=version,
                filename=filename,
                is_regridded=is_regridded,
                is_top_level=is_top_level,
                time_start=time_start,
                time_end=time_end,
                year_start=year_start,
                year_end=year_end,
            )
        except (IndexError, ValueError) as e:
            return None


@dataclass
class DataCatalog:
    """Catalog of all CMIP6 files in the data directory."""
    
    data_root: Path
    files: List[FileMetadata] = field(default_factory=list)
    _by_key: Dict[str, List[FileMetadata]] = field(default_factory=lambda: defaultdict(list))
    
    def __post_init__(self):
        """Scan directory and build catalog."""
        if self.files:
            self._build_index()
    
    def scan(self, pattern: str = "*.nc") -> None:
        """Scan data_root for all NetCDF files and build catalog."""
        rich_print(f"[blue]Scanning {self.data_root} for {pattern} files...[/blue]")
        
        nc_files = list(self.data_root.rglob(pattern))
        rich_print(f"[blue]Found {len(nc_files)} files, parsing metadata...[/blue]")
        
        self.files = []
        for path in nc_files:
            metadata = FileMetadata.from_path(path)
            if metadata:
                self.files.append(metadata)
        
        self._build_index()
        rich_print(f"[green]Catalog built: {len(self.files)} valid CMIP6 files[/green]")
    
    def _build_index(self) -> None:
        """Build internal indexes for fast lookup."""
        self._by_key.clear()
        
        for file in self.files:
            # Create composite key: mip/institute/model/experiment/variant/frequency/variable/grid/version
            key = f"{file.mip}/{file.institute}/{file.model}/{file.experiment}/{file.variant}/{file.frequency}/{file.variable}/{file.grid}/{file.version}"
            self._by_key[key].append(file)
    
    def get_files(
        self,
        mip: Optional[str] = None,
        institute: Optional[str] = None,
        model: Optional[str] = None,
        experiment: Optional[str] = None,
        variant: Optional[str] = None,
        variable: Optional[str] = None,
        frequency: Optional[str] = None,
        is_regridded: Optional[bool] = True,
        is_top_level: Optional[bool] = None,
        year_min: Optional[int] = None,
        year_max: Optional[int] = None,
    ) -> List[FileMetadata]:
        """Get files matching specified criteria."""
        results = self.files.copy()
        
        filters = {
            'mip': mip,
            'institute': institute,
            'model': model,
            'experiment': experiment,
            'variant': variant,
            'variable': variable,
            'frequency': frequency,
        }
        
        # apply filters
        for attr, value in filters.items():
            if value is not None:
                results = [f for f in results if getattr(f, attr) == value]
        
        if is_regridded is not None:
            results = [f for f in results if f.is_regridded == is_regridded]
        if is_top_level is not None:
            if variable not in ["tos", "rsdo"]: # these are single-level variables
                results = [f for f in results if f.is_top_level == is_top_level]
        if year_min is not None:
            results = [f for f in results if f.year_end is None or f.year_end >= year_min]
        if year_max is not None:
            results = [f for f in results if f.year_start is None or f.year_start <= year_max]
        
        return results
    
    def get_overview(self) -> Dict:
        """Get comprehensive overview of available data."""
        if not self.files:
            return {}
        
        # Get unique values for each dimension
        overview = {
            'total_files': len(self.files),
            'mips': sorted(set(f.mip for f in self.files)),
            'institutes': sorted(set(f.institute for f in self.files)),
            'models': sorted(set(f.model for f in self.files)),
            'experiments': sorted(set(f.experiment for f in self.files)),
            'variants': sorted(set(f.variant for f in self.files)),
            'variables': sorted(set(f.variable for f in self.files)),
            'frequencies': sorted(set(f.frequency for f in self.files)),
            'regridded_count': sum(1 for f in self.files if f.is_regridded),
            'top_level_count': sum(1 for f in self.files if f.is_top_level),
            'year_range': self._get_year_range(),
            'by_experiment': self._count_by_experiment(),
            'by_variable': self._count_by_variable(),
            'by_model': self._count_by_model(),
        }
        
        return overview
    
    def _get_year_range(self) -> Dict[str, Optional[int]]:
        """Get overall year range across all files."""
        years = []
        for f in self.files:
            if f.year_start:
                years.append(f.year_start)
            if f.year_end:
                years.append(f.year_end)
        
        return {
            'min': min(years) if years else None,
            'max': max(years) if years else None,
        }
    
    def _count_by_experiment(self) -> Dict[str, int]:
        """Count files by experiment."""
        counts = defaultdict(int)
        for f in self.files:
            counts[f.experiment] += 1
        return dict(counts)
    
    def _count_by_variable(self) -> Dict[str, int]:
        """Count files by variable."""
        counts = defaultdict(int)
        for f in self.files:
            counts[f.variable] += 1
        return dict(counts)
    
    def _count_by_model(self) -> Dict[str, int]:
        """Count files by model."""
        counts = defaultdict(int)
        for f in self.files:
            counts[f.model] += 1
        return dict(counts)
    
    def print_overview(self, console: Optional[Console] = None) -> None:
        """Print a formatted overview table."""
        if console is None:
            console = Console()
        
        overview = self.get_overview()
        
        # Main statistics table
        table = Table(title="[cyan]CMIP6 Data Overview[/cyan]", show_header=True)
        table.add_column("Metric", style="bold")
        table.add_column("Value", style="bold")
        
        table.add_row("Total Files", str(overview['total_files']))
        table.add_row("Regridded Files", str(overview['regridded_count']))
        table.add_row("Top Level Files", str(overview['top_level_count']))
        
        year_range = overview['year_range']
        if year_range['min'] and year_range['max']:
            table.add_row("Year Range", f"{year_range['min']} - {year_range['max']}")
        else:
            table.add_row("Year Range", "Unknown")
        
        table.add_row("", "")  # Empty row
        table.add_row("MIPs", ", ".join(overview['mips']))
        table.add_row("Experiments", ", ".join(overview['experiments']))
        table.add_row("Variables", ", ".join(overview['variables']))
        table.add_row("Models", ", ".join(overview['models'][:10]))  # First 10 models
        if len(overview['models']) > 10:
            table.add_row("", f"... and {len(overview['models']) - 10} more")
        
        console.print(table)
        
        # Experiment breakdown
        exp_table = Table(title="[cyan]Files by Experiment[/cyan]", show_header=True)
        exp_table.add_column("Experiment", style="bold")
        exp_table.add_column("Count", justify="right")
        for exp, count in sorted(overview['by_experiment'].items()):
            exp_table.add_row(exp, str(count))
        console.print(exp_table)
        
        # Variable breakdown
        var_table = Table(title="[cyan]Files by Variable[/cyan]", show_header=True)
        var_table.add_column("Variable", style="bold")
        var_table.add_column("Count", justify="right")
        for var, count in sorted(overview['by_variable'].items()):
            var_table.add_row(var, str(count))
        console.print(var_table)


@dataclass
class SourceDataset:
    """Container for a dataset from a single source (institute/model/variant)."""
    source_key: str  # e.g., "AWI/AWI-CM-1-1-MR/r1i1p1f1"
    institute: str
    model: str
    variant: str
    version: str
    dataset: xa.Dataset
    variable: str
    experiment: str
    
    def __repr__(self) -> str:
        return f"SourceDataset(source={self.source_key}, variable={self.variable}, experiment={self.experiment})"


class CMIP6DataLoader:
    """Main class for loading CMIP6 data into xarray datasets."""
    
    def __init__(self, data_root: Union[str, Path], prefer_regridded: bool = True, prefer_top_level: bool = True):
        """
        Initialize the data loader.
        
        Args:
            data_root: Root directory containing CMIP6 data
            prefer_regridded: If True, prefer regridded files when both exist
            prefer_top_level: If True, prefer top_level files when both exist
        """
        self.data_root = Path(data_root)
        self.prefer_regridded = prefer_regridded
        self.prefer_top_level = prefer_top_level
        self.catalog = DataCatalog(data_root=self.data_root)
        
    def scan_catalog(self) -> None:
        """Scan and build the file catalog."""
        self.catalog.scan()
    
    def _group_files_by_source(self, files: List[FileMetadata]) -> Dict[str, List[FileMetadata]]:
        """Group files by their source (institute/model/variant combination)."""
        grouped = defaultdict(list)
        for file in files:
            source_key = file.get_source_key()
            grouped[source_key].append(file)
        
        # Sort files within each group by time
        for source_key in grouped:
            grouped[source_key].sort(key=lambda f: (f.time_start or 0, f.path))
        
        return dict(grouped)
    
    def load_variable(
        self,
        variable: str,
        experiment: Optional[str] = None,
        model: Optional[str] = None,
        variant: Optional[str] = None,
        institute: Optional[str] = None,
        mip: Optional[str] = None,
        frequency: Optional[str] = None,
        year_start: Optional[int] = None,
        year_end: Optional[int] = None,
        chunks: Optional[Dict] = None,
        return_sources: bool = True,
    ) -> Union[Dict[str, SourceDataset], xa.Dataset]:
        """
        Load a variable, grouping files by source (institute/model/variant).
        
        Files from different sources are loaded separately and returned as a
        dictionary of SourceDataset objects. Files from the same source are
        concatenated along the time dimension.
        
        Args:
            variable: Variable name (e.g., 'tos', 'no3')
            experiment: Experiment name (e.g., 'historical', 'ssp585')
            model: Model name (e.g., 'CanESM5')
            variant: Variant ID (e.g., 'r1i1p1f1')
            institute: Institute name
            mip: MIP type (e.g., 'CMIP', 'ScenarioMIP')
            frequency: Frequency (e.g., 'Omon')
            year_start: Start year filter
            year_end: End year filter
            chunks: Dask chunking dictionary for lazy loading
            return_sources: If True, return dict of SourceDataset; if False, return single Dataset (only if one source)
            
        Returns:
            Dictionary mapping source keys to SourceDataset objects, or single Dataset if return_sources=False and only one source
        """
        # Get matching files
        files = self.catalog.get_files(
            variable=variable,
            experiment=experiment,
            model=model,
            variant=variant,
            institute=institute,
            mip=mip,
            frequency=frequency,
            is_regridded=self.prefer_regridded,
            is_top_level=self.prefer_top_level if self.prefer_top_level is not None else None,
            year_min=year_start,
            year_max=year_end,
        )
        
        if not files:
            raise ValueError(
                f"No files found matching criteria: variable={variable}, "
                f"experiment={experiment}, model={model}, variant={variant}"
            )
        
        # Group files by source
        grouped_files = self._group_files_by_source(files)
        
        # If only one source and return_sources=False, return single dataset
        if not return_sources and len(grouped_files) == 1:
            source_key = list(grouped_files.keys())[0]
            source_files = grouped_files[source_key]
            file_paths = [f.path for f in source_files]
            ds = self._load_files_for_source(file_paths, chunks)
            return ds
        
        # Load each source separately
        results = {}
        
        for source_key, source_files in grouped_files.items():
            # Get source metadata from first file
            first_file = source_files[0]
            source_info = first_file.get_source_id()
            
            file_paths = [f.path for f in source_files]
            
            try:
                ds = self._load_files_for_source(file_paths, chunks)
                # TODO:tmu fix cftime.Datetime360Day(2015, 1, 16, 0, 0, 0, 0, has_year_zero=True) issue with datetime, by preprocessing the formed datasets
                
                # Apply time slicing if year range specified
                if year_start is not None or year_end is not None:
                    time_start = esgpull_utils.to_time_slice_value(year_start, default_start=True) if year_start else None
                    time_end = esgpull_utils.to_time_slice_value(year_end, default_start=False) if year_end else None
                    ds = ds.sel(time=slice(time_start, time_end))
                
                source_dataset = SourceDataset(
                    source_key=source_key,
                    institute=source_info['institute'],
                    model=source_info['model'],
                    variant=source_info['variant'],
                    version=source_info['version'],
                    dataset=ds,
                    variable=variable,
                    experiment=experiment or first_file.experiment,
                )
                results[source_key] = source_dataset
                
                file_str = "file" if len(source_files) == 1 else "files"
                rich_print(f"[green]Loaded {variable} from {source_key} ({len(source_files)} {file_str})[/green]")
            except Exception as e:
                rich_print(f"[red]Failed to load {variable} from {source_key}: {e}[/red]")
                continue
        
        if not results:
            raise ValueError(f"No sources could be loaded for variable {variable}")
        
        return results
    
    def _load_files_for_source(self, file_paths: List[Path], chunks: Optional[Dict] = None) -> xa.Dataset:
        """Load and concatenate files from a single source."""
        if len(file_paths) == 1:
            # Single file - just open it
            return esgpull_utils.process_xa_d(xa.open_dataset(file_paths[0], chunks=chunks, decode_times=True))
        
        # Multiple files - try automatic concatenation first
        try:
            ds = xa.open_mfdataset(
                file_paths,
                combine='by_coords',  # Automatically align by coordinates
                chunks=chunks,  # Lazy loading with Dask
                decode_times=True,
                parallel=True,
            )
            # process dataset for future handling
            return esgpull_utils.process_xa_d(ds)
        except Exception as e:
            # Fallback to manual concatenation if automatic fails
            rich_print(f"[yellow]Warning: Automatic concatenation failed: {e}[/yellow]")
            rich_print(f"[yellow]Attempting manual concatenation...[/yellow]")
            return self._manual_concat(file_paths, chunks)
    
    def _manual_concat(self, file_paths: List[Path], chunks: Optional[Dict] = None) -> xa.Dataset:
        """Manually concatenate files along time dimension."""
        datasets = []
        
        for path in file_paths:
            try:
                ds = xa.open_dataset(path, chunks=chunks, decode_times=True)
                datasets.append(ds)
            except Exception as e:
                rich_print(f"[red]Warning: Failed to load {path.name}: {e}[/red]")
                continue
        
        if not datasets:
            raise ValueError("No datasets could be loaded")
        
        # Concatenate along time
        combined = xa.concat(datasets, dim='time')
        
        # Close individual datasets to free memory
        for ds in datasets:
            ds.close()
        
        return combined
    
    def load_ensemble_variables(
        self,
        experiment: str,
        variables: List[str],
        variant: Optional[str] = None,
        model: Optional[str] = None,
        year_start: Optional[int] = None,
        year_end: Optional[int] = None,
        chunks: Optional[Dict] = None,
    ) -> Dict[str, Dict[str, SourceDataset]]:
        """
        Load multiple variables for an ensemble, grouped by source.
        
        Args:
            experiment: Experiment name (e.g., 'historical')
            variables: List of variable names to load
            variant: Variant ID (e.g., 'r1i1p1f1')
            model: Model name
            year_start: Start year filter
            year_end: End year filter
            chunks: Dask chunking dictionary
            
        Returns:
            Dictionary mapping variable names to dictionaries of SourceDataset objects
            Structure: {variable: {source_key: SourceDataset}}
        """
        results = {}
        
        for variable in variables:
            try:
                sources = self.load_variable(
                    variable=variable,
                    experiment=experiment,
                    variant=variant,
                    model=model,
                    year_start=year_start,
                    year_end=year_end,
                    chunks=chunks,
                    return_sources=True,
                )
                results[variable] = sources
                rich_print(f"[green]Loaded {variable} for {experiment} ({len(sources)} sources)[/green]")
            except Exception as e:
                rich_print(f"[red]Failed to load {variable}: {e}[/red]")
                continue
        
        return results
    
    def load_all_historical_ensembles(
        self,
        variables: List[str],
        year_start: Optional[int] = None,
        year_end: Optional[int] = None,
        chunks: Optional[Dict] = None,
    ) -> Dict[str, Dict[str, SourceDataset]]:
        """
        Load all historical sources (institute/model/variant combinations) with all specified variables.
        
        Structure: {source_key: {variable: SourceDataset}}
        Useful for ensemble ML training where each source is a separate ensemble member.
        
        Args:
            variables: List of variable names to load
            year_start: Start year filter
            year_end: End year filter
            chunks: Dask chunking dictionary
            
        Returns:
            Nested dictionary: {source_key: {variable: SourceDataset}}
        """
        # Get all unique sources for historical experiment
        historical_files = self.catalog.get_files(
            experiment='historical',
            is_regridded=self.prefer_regridded,
        )
        
        # Get unique sources (institute/model/variant combinations)
        sources = sorted(set(f.get_source_key() for f in historical_files))
        rich_print(f"[blue]Found {len(sources)} historical sources[/blue]")
        
        results = {}
        
        # Load all variables for each source
        for source_key in sources:
            # Extract institute/model/variant from source_key
            parts = source_key.split('/')
            if len(parts) != 3:
                continue
            institute, model, variant = parts
            
            rich_print(f"[cyan]Loading source {source_key}...[/cyan]")
            
            source_results = {}
            for variable in variables:
                try:
                    sources_dict = self.load_variable(
                        variable=variable,
                        experiment='historical',
                        institute=institute,
                        model=model,
                        variant=variant,
                        year_start=year_start,
                        year_end=year_end,
                        chunks=chunks,
                        return_sources=True,
                    )
                    # Should only be one source in the dict (the one we asked for)
                    if source_key in sources_dict:
                        source_results[variable] = sources_dict[source_key]
                except Exception as e:
                    rich_print(f"[yellow]Warning: Failed to load {variable} for {source_key}: {e}[/yellow]")
                    continue
            
            if source_results:  # Only add if we got some data
                results[source_key] = source_results
        
        return results
    
    def get_available_combinations(
        self,
        experiment: Optional[str] = None,
    ) -> Dict[str, List[str]]:
        """
        Get all available combinations for filtering.
        
        Returns:
            Dictionary with keys: 'models', 'variants', 'variables', 'experiments', etc.
        """
        files = self.catalog.get_files(
            experiment=experiment,
            is_regridded=self.prefer_regridded,
        )
        
        return {
            'models': sorted(set(f.model for f in files)),
            'institutes': sorted(set(f.institute for f in files)),
            'variants': sorted(set(f.variant for f in files)),
            'variables': sorted(set(f.variable for f in files)),
            'experiments': sorted(set(f.experiment for f in files)),
            'frequencies': sorted(set(f.frequency for f in files)),
        }


# Convenience function
def create_loader(data_root: Union[str, Path], scan: bool = True, **kwargs) -> CMIP6DataLoader:
    """Create and optionally scan a CMIP6DataLoader."""
    loader = CMIP6DataLoader(data_root, **kwargs)
    if scan:
        loader.scan_catalog()
    return loader


# ================================
# Example Usage
# ================================
"""
Example 1: Overview of downloaded data

    from esgpull.esgpullplus.load_data import create_loader
    
    # Create loader and scan data directory
    loader = create_loader('/maps/rt582/data/CMIP6')
    
    # Print overview
    loader.catalog.print_overview()
    
    # Get detailed overview as dictionary
    overview = loader.catalog.get_overview()
    print(f"Available variables: {overview['variables']}")
    print(f"Year range: {overview['year_range']}")


Example 2: Load a specific variable for a scenario (grouped by source)

    # Load sea surface temperature for SSP585 scenario
    # Returns dict of SourceDataset objects, one per unique source
    sources = loader.load_variable(
        variable='tos',
        experiment='ssp585',
        year_start=2015,
        year_end=2050,
    )
    
    # Access individual sources
    for source_key, source_ds in sources.items():
        print(f"{source_key}: {source_ds.dataset.dims}")
        # Access the actual dataset
        ds = source_ds.dataset
        print(f"  Institute: {source_ds.institute}")
        print(f"  Model: {source_ds.model}")
        print(f"  Variant: {source_ds.variant}")
    
    # If you know there's only one source, you can get a single dataset
    ds = loader.load_variable(
        variable='tos',
        experiment='ssp585',
        model='CanESM5',
        variant='r1i1p1f1',
        return_sources=False,  # Returns single Dataset if only one source
    )


Example 3: Load multiple variables for an ensemble (ML training prep)

    # Load multiple variables for historical experiment
    # Returns: {variable: {source_key: SourceDataset}}
    variables = ['tos', 'no3', 'so', 'thetao']
    ensemble_data = loader.load_ensemble_variables(
        experiment='historical',
        variables=variables,
        variant='r1i1p1f1',
        model='CanESM5',
        year_start=1850,
        year_end=2014,
    )
    
    # Access individual variables and sources
    for variable, sources_dict in ensemble_data.items():
        for source_key, source_ds in sources_dict.items():
            print(f"{variable} from {source_key}: {source_ds.dataset.dims}")


Example 4: Load all historical sources for ML training

    # Load all historical sources (institute/model/variant combinations) with all variables
    # Returns: {source_key: {variable: SourceDataset}}
    all_ensembles = loader.load_all_historical_ensembles(
        variables=['tos', 'no3', 'so', 'thetao'],
        year_start=1850,
        year_end=2014,
    )
    
    # Structure: {source_key: {variable: SourceDataset}}
    for source_key, variables in all_ensembles.items():
        print(f"Source {source_key} has {len(variables)} variables")
        for var_name, source_ds in variables.items():
            print(f"  {var_name}: {source_ds.dataset.dims}")
            # Access the actual dataset
            ds = source_ds.dataset


Example 5: Query what's available

    # Get all available combinations
    available = loader.get_available_combinations(experiment='historical')
    print(f"Available models: {available['models']}")
    print(f"Available variants: {available['variants']}")
    
    # Get specific files matching criteria
    files = loader.catalog.get_files(
        variable='tos',
        experiment='historical',
        year_min=1900,
        year_max=2000,
    )
    print(f"Found {len(files)} files")


Example 6: Custom file filtering and loading

    # Get files for a specific combination
    files = loader.catalog.get_files(
        experiment='ssp585',
        variable='no3',
        is_regridded=True,
        is_top_level=True,
    )
    
    # Sort by time
    files.sort(key=lambda f: f.time_start or 0)
    
    # Load manually if needed
    import xarray as xa
    datasets = [xa.open_dataset(f.path) for f in files[:5]]  # First 5 files
    combined = xa.concat(datasets, dim='time')
"""
