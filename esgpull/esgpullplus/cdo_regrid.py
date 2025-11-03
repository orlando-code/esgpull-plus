#!/usr/bin/env python3
"""
CDO regridding pipeline with comprehensive functionality.

This module provides a complete regridding solution that:
- Handles all grid types (structured, curvilinear, unstructured)
- Provides memory optimization and chunked processing
- Supports parallel processing and weight reuse
- Includes comprehensive error handling and monitoring
- Offers both basic and advanced usage patterns
"""

import hashlib
import tempfile
import shutil
import multiprocessing as mp
import psutil
from pathlib import Path
from typing import Optional
import logging
import traceback
from concurrent.futures import ProcessPoolExecutor

import time
# import numpy as np
import xarray as xa
from cdo import Cdo
from rich.console import Console
# from rich.progress import Progress, SpinnerColumn, TextColumn, BarColumn, TimeElapsedColumn, TimeRemainingColumn    # TODO: make a nice processing UI
# from rich.live import Live
from rich.panel import Panel
from rich.table import Table

from esgpull.esgpullplus import utils, fileops
from esgpull.esgpullplus.regrid_ui import RegridProgressUI, BatchRegridUI


def _process_single_file_standalone(
    file_path: Path,
    output_dir: Optional[Path],
    target_resolution: tuple[float, float],
    target_grid: str,
    weight_cache_dir: Path,
    top_level_only: bool,
    max_memory_gb: float,
    chunk_size_gb: float,
    enable_chunking: bool,
    overwrite: bool = False,
    representative_file: Optional[Path] = None,
    verbose: bool = False,
) -> dict[str, any]:
    """
    Standalone function for processing a single file in parallel.
    Creates its own pipeline instance to avoid pickle issues.
    
    Args:
    - file_path (Path): Path to the input file
    - output_dir (Optional[Path]): Output directory for regridded files
    - target_resolution (tuple[float, float]): Target resolution as (lon_res, lat_res)
    - target_grid (str): Target grid type ('lonlat', 'gaussian', etc.)
    - weight_cache_dir (Path): Directory to cache regrid weights
    - top_level_only (bool): If True, only process the top level for multi-level files
    - max_memory_gb (float): Maximum memory usage in GB
    - chunk_size_gb (float): Maximum chunk size in GB
    - enable_chunking (bool): If True, chunk the file for processing
    - overwrite (bool): If True, overwrite existing output files
    - representative_file (Optional[Path]): Representative file for resolution calculation
    
    Returns:
    - dict[str, any]: Dictionary containing the result of the regridding
        - 'success': Boolean indicating if the regridding was successful
        - 'file_path': Path to the input file
        - 'skipped': Boolean indicating if the file was skipped
        - 'message': Message indicating the result of the regridding
        - 'stats': Dictionary containing the statistics of the regridding
            - 'files_processed': Number of files processed
            - 'weights_reused': Number of weights reused
            - 'weights_generated': Number of weights generated
            - 'chunks_processed': Number of chunks processed
            - 'errors': Number of errors
            - 'total_size_gb': Total size of the regridded files in GB
            - 'memory_peak_gb': Peak memory usage in GB
            - 'grid_types': Dictionary containing the grid types of the regridded files
                - 'structured': Number of structured grids
                - 'curvilinear': Number of curvilinear grids
                - 'unstructured_ncells': Number of unstructured grids
                - 'unknown': Number of unknown grids
    """
    # create a new pipeline instance for a specific worker
    pipeline = CDORegridPipeline(
        target_resolution=target_resolution,
        target_grid=target_grid,
        weight_cache_dir=weight_cache_dir,
        top_level_only=top_level_only,
        verbose=verbose,  # use the passed verbose parameter
        max_memory_gb=max_memory_gb,
        chunk_size_gb=chunk_size_gb,
        enable_parallel=False,  # disable parallel in workers (we are already parallelizing)
        enable_chunking=enable_chunking,
        memory_monitoring=False,  # disable memory monitoring in workers (we are already monitoring memory)
    )
    
    # set representative file for resolution calculation
    if representative_file:
        pipeline._representative_file = representative_file
    
    try:
        # Use lightweight check for has_level to avoid expensive full file analysis
        has_level = pipeline._has_level_lightweight(file_path)
        # determine output path
        if output_dir:
            output_filename = pipeline._generate_output_filename(file_path, has_level, top_level_only)
            output_path = output_dir / output_filename
        else:
            output_filename = pipeline._generate_output_filename(file_path, has_level, top_level_only)
            output_path = file_path.parent / output_filename
        
        # check if output already exists (unless overwrite is True)
        if output_path.exists() and not overwrite:
            return {
                'success': True,
                'file_path': file_path,
                'skipped': True,
                'message': 'File already exists',
                'stats': {
                    'files_processed': 0,  # skipped files don't count as processed
                    'weights_reused': 0,
                    'weights_generated': 0,
                    'chunks_processed': 0,
                    'errors': 0,
                    'total_size_gb': 0.0,
                    'memory_peak_gb': 0.0,
                    'grid_types': {}
                }
            }
        
        # regrid file
        success = pipeline.regrid_file(file_path, output_path, overwrite=overwrite)
        
        # collect statistics from the worker's pipeline
        worker_stats = pipeline.stats.copy()
        
        if success:
            return {
                'success': True,
                'file_path': file_path,
                'skipped': False,
                'message': 'Successfully regridded',
                'stats': worker_stats
            }
        else:
            return {
                'success': False,
                'file_path': file_path,
                'skipped': False,
                'message': 'Regridding failed',
                'stats': worker_stats
            }
            
    except Exception as e:
        return {
            'success': False,
            'file_path': file_path,
            'skipped': False,
            'message': f'Error: {str(e)}',
            'stats': {
                'files_processed': 0,
                'weights_reused': 0,
                'weights_generated': 0,
                'chunks_processed': 0,
                'errors': 1,
                'total_size_gb': 0.0,
                'memory_peak_gb': 0.0,
                'grid_types': {}
            }
        }


class CDORegridPipeline:
    """
    CDO regridding pipeline with all advanced features.
    """
    
    def __init__(
        self,
        target_resolution: tuple[float, float] = (1.0, 1.0),    # TODO: automate this based on native resolution of input file
        target_grid: str = "lonlat",
        weight_cache_dir: Optional[Path] = None,
        top_level_only: bool = True,    # TODO: add seafloor option
        verbose: bool = True,
        max_memory_gb: float = 8.0,
        max_workers: Optional[int] = 16,
        chunk_size_gb: float = 2.0,
        enable_parallel: bool = True,
        enable_chunking: bool = True,
        memory_monitoring: bool = True,
        cleanup_weights: bool = False,
    ):
        """
        Initialize the CDO regridding pipeline.
        
        Parameters
        ----------
        target_resolution (tuple): Target resolution as (lon_res, lat_res) in degrees.
        target_grid (str): Target grid type ('lonlat', 'gaussian', etc.).
        weight_cache_dir (Path, optional): Directory to cache regrid weights.
        top_level_only (bool): If True, only process the top level for multi-level files.
        verbose (bool): Enable verbose output.
        max_memory_gb (float): Maximum memory usage in GB.
        max_workers (int, optional): Maximum number of parallel workers.
        chunk_size_gb (float): Maximum chunk size in GB for large files.
        enable_parallel (bool): Enable parallel processing.
        enable_chunking (bool): Enable chunked processing for large files.
        memory_monitoring (bool): Enable memory usage monitoring.
        """
        self.target_resolution = target_resolution
        self.target_grid = target_grid
        self.top_level_only = top_level_only
        self.verbose = verbose
        self.max_memory_gb = max_memory_gb
        self.chunk_size_gb = chunk_size_gb
        self.enable_parallel = enable_parallel
        self.enable_chunking = enable_chunking
        self.memory_monitoring = memory_monitoring
        self.cleanup_weights = cleanup_weights
                
        # ensure not requesting more workers than available
        if max_workers is None:
            self.max_workers = min(self.max_workers, mp.cpu_count())
        else:
            self.max_workers = max_workers
        
        # set up CDO with optimized settings
        self.cdo = self._setup_cdo()
        
        # set up console and logger for output
        self.console = Console()
        self.logger = self._setup_logger()
        
        # weight cache management
        self.weight_cache_dir = weight_cache_dir or Path.cwd() / "cdo_weights"  # XXXTODO: make this local to each directory of files being regridded
        self.weight_cache_dir.mkdir(exist_ok=True) if self.weight_cache_dir else None
        self.weight_cache: dict[str, Path] = {}
        self.cleanup_weight_files() if self.weight_cache_dir.exists() and self.cleanup_weights else None    # TODO: check that this works
        
        # monitor memory to prevent antisocial behaviour on shared machines andout of memory errors
        self.memory_monitor = MemoryMonitor() if memory_monitoring else None
        
        # prepare to store meta-data and processing statistics
        self.stats = {
            'files_processed': 0,
            'weights_reused': 0,
            'weights_generated': 0,
            'errors': 0,
            'total_size_gb': 0.0,
            'chunks_processed': 0,
            'memory_peak_gb': 0.0,
            'grid_types': {
                'structured': 0,
                'curvilinear': 0,
                'tripolar_ocean': 0,
                'unstructured_ncells': 0,
                'unknown': 0,
            }
        }
        
        # cache for file info to avoid repeated expensive operations
        self._file_info_cache: dict[Path, dict] = {}
        
        # track created files for cleanup on interrupt
        self._created_files: list[Path] = []
        self._setup_signal_handlers()
        
        # timing tracking
        self._start_time: Optional[time.struct_time] = None
        self.end_time: Optional[time.struct_time] = None
        
    def _prune_regridded(self, input_files: list[Path], overwrite: bool = False) -> list[Path]:
        """Prune files with 'regridded' in the name to avoid processing them twice."""
        if self.prune_regridded:
            if overwrite:   # delete existing regridded files
                for file in input_files:
                    if 'regridded' in file.name:
                        if self.verbose:
                            self.console.print(f"[yellow]Removing existing regridded file: {file.name}[/yellow]")
                        file.unlink()
            else:   # skip existing regridded files (don't regenerate)
                if self.verbose:
                    regridded_files = [file for file in input_files if 'regridded' in file.name]
                    if regridded_files:
                        self.console.print(f"[blue]Skipping {len(regridded_files)} existing regridded files (overwrite=False)[/blue]")
        return [file for file in input_files if 'regridded' in file.name and not "_chunk_" in file.name]
    
    def _cleanup_files_by_pattern(
        self, 
        input_files: list[Path], 
        pattern: str,
        file_type: str,
        exclude_regridded: bool = False
    ) -> list[Path]:
        """Generic helper to clean up files matching a pattern.
        
        Args:
            input_files: List of file paths to check
            pattern: Pattern to match in filename (e.g., '_top_level', '_chunk_')
            file_type: Description for logging (e.g., '_top_level', '_chunk_')
            exclude_regridded: If True, exclude files with 'regridded' in name
            
        Returns:
            List of cleaned files (with problematic files removed)
        """
        cleaned_files = []
        removed_count = 0
        
        for file_path in input_files:
            should_remove = pattern in file_path.name
            if exclude_regridded and should_remove:
                should_remove = "regridded" not in file_path.name
            
            if should_remove:
                if self.verbose:
                    self.console.print(
                        f"[yellow]Removing problematic {file_type} file: {file_path.name}[/yellow]"
                    )
                try:
                    file_path.unlink()
                    removed_count += 1
                except Exception as e:
                    self.logger.warning(f"Could not remove {file_path}: {e}")
            else:
                cleaned_files.append(file_path)
        
        if removed_count > 0 and self.verbose:
            self.console.print(
                f"[blue]Cleaned up {removed_count} problematic {file_type} files[/blue]"
            )
        
        return cleaned_files
    
    def _cleanup_top_level_files(self, input_files: list[Path]) -> list[Path]:
        """Clean up existing _top_level files that may cause HDF errors."""
        # TODO: why do I need to get rid of top_level files?
        return self._cleanup_files_by_pattern(
            input_files, 
            pattern='_top_level',
            file_type='_top_level',
            exclude_regridded=True
        )
    
    def _cleanup_chunk_files(self, input_files: list[Path]) -> list[Path]:
        """Clean up existing _chunk_ files that may cause HDF errors."""
        return self._cleanup_files_by_pattern(
            input_files,
            pattern='_chunk_',
            file_type='_chunk_',
            exclude_regridded=False
        )
    
    def _cleanup_problematic_files(self, input_files: list[Path]) -> list[Path]:
        """Clean up all problematic files (_top_level and _chunk_) that may cause HDF errors."""
        return self._cleanup_files_by_pattern(
            input_files,
            pattern='_top_level',
            file_type='_top_level',
            exclude_regridded=True # keep regridded top_level files (final form)
        ) + self._cleanup_files_by_pattern(
            input_files,
            pattern='_chunk_',
            file_type='_chunk_',
            exclude_regridded=False # get rid of everything that's still a chunk
        )
    
    def _setup_signal_handlers(self):
        """Set up signal handlers for graceful cleanup on keyboard interrupt."""
        import signal
        
        def signal_handler(signum, frame):
            """Handle keyboard interrupt and cleanup created files."""
            if self.verbose:
                self.console.print(f"\n[yellow]Received interrupt signal ({signum}). Cleaning up...[/yellow]")
            
            self._cleanup_created_files()
            
            if self.verbose:
                self.console.print(f"[red]Interrupted. Cleaned up {len(self._created_files)} created files.[/red]")
            
            # exit gracefully
            import sys
            sys.exit(1)
        
        # register signal handlers
        signal.signal(signal.SIGINT, signal_handler)  # Ctrl+C
        signal.signal(signal.SIGTERM, signal_handler)  # termination signal
    
    def _cleanup_created_files(self):
        """Clean up all files created during processing."""
        for file_path in self._created_files:
            try:
                if file_path.exists():
                    file_path.unlink()
                    if self.verbose:
                        self.logger.info(f"Cleaned up created file: {file_path.name}")
            except Exception as e:
                self.logger.warning(f"Could not clean up {file_path}: {e}")
        
        self._created_files.clear()
    
    def _track_created_file(self, file_path: Path):
        """Track a file that was created during processing for cleanup."""
        self._created_files.append(file_path)
    
    def _setup_cdo(self) -> Cdo:
        """Set up CDO with optimized performance settings."""
        import os
        
        # set CDO environment variables for optimal performance
        os.environ["CDO_NETCDF_COMPRESSION"] = "0"  # disable compression for speed TODO: compress if space becomes an issue
        os.environ["CDO_NETCDF_64BIT_OFFSET"] = "1"  # use 64-bit offsets for large files
        os.environ["CDO_NETCDF_USE_PARALLEL"] = "1" if self.enable_parallel else "0" # enable parallel I/O 
        os.environ["CDO_NUM_THREADS"] = str(self.max_workers)
        
        return Cdo()
    
    def _setup_logger(self) -> logging.Logger:
        """Set up logging for the pipeline."""
        # TODO: understand
        logger = logging.getLogger(f"cdo_regrid_{id(self)}")   # TODO: check that this functions for directories
        logger.setLevel(logging.INFO)
        
        if not logger.handlers:
            handler = logging.StreamHandler()
            formatter = logging.Formatter(
                '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
            )
            handler.setFormatter(formatter)
            logger.addHandler(handler)
        
        return logger
    
    def _detect_grid_type(self, ds: xa.Dataset, dims: dict) -> str:
        """
        Detect the type of grid structure with comprehensive support.
        
        Returns:
        - 'unstructured_ncells': Grid with ncells dimension (true unstructured)
        - 'tripolar_ocean': Tri-polar ocean grid with (y,x) dimensions, 2D lat/lon coords, and cell boundaries
        - 'curvilinear': Grid with (j,i) dimensions and 2D lat/lon coordinates
        - 'structured': Regular lon/lat grid with 1D coordinates
        - 'unknown': Cannot determine grid type
        """
        # check for true unstructured grid (ncells dimension)
        if 'ncells' in dims:
            return 'unstructured_ncells'
        
        # check for tri-polar ocean grid (y,x dimensions with 2D lat/lon coords and cell boundaries)
        if 'y' in dims and 'x' in dims:
            lat_coords = ['lat', 'latitude']
            lon_coords = ['lon', 'longitude']
            
            has_2d_lat = any(coord in ds.coords and ds[coord].ndim == 2 for coord in lat_coords)
            has_2d_lon = any(coord in ds.coords and ds[coord].ndim == 2 for coord in lon_coords)
            
            if has_2d_lat and has_2d_lon:
                # Check for cell boundaries (indicative of tri-polar ocean grids)
                has_bounds_lat = ('bounds_lat' in list(ds.coords) or 'bounds_lat' in list(ds.data_vars))
                has_bounds_lon = ('bounds_lon' in list(ds.coords) or 'bounds_lon' in list(ds.data_vars))
                has_nvertex = 'nvertex' in dims
                
                # Check for ocean-specific variables
                ocean_vars = ['so', 'thetao', 'uo', 'vo', 'wo', 'zos', 'mlotst', 'sithick']
                has_ocean_vars = any(var in ds.data_vars for var in ocean_vars)
                
                if (has_bounds_lat and has_bounds_lon and has_nvertex) or has_ocean_vars:
                    return 'tripolar_ocean'
                else:
                    return 'curvilinear'
        
        # check for curvilinear grid (j,i dimensions with 2D lat/lon coords)
        if 'j' in dims and 'i' in dims:
            # check if lat/lon are 2D coordinates on (j,i) grid
            lat_coords = ['lat', 'latitude']
            lon_coords = ['lon', 'longitude']
            
            has_2d_lat = any(coord in ds.coords and ds[coord].ndim == 2 for coord in lat_coords)
            has_2d_lon = any(coord in ds.coords and ds[coord].ndim == 2 for coord in lon_coords)
            
            if has_2d_lat and has_2d_lon:
                return 'curvilinear'
        
        # check for structured grid (1D lat/lon coordinates)
        lat_coords = ['lat', 'latitude']
        lon_coords = ['lon', 'longitude']
        
        has_1d_lat = any(coord in ds.coords and ds[coord].ndim == 1 for coord in lat_coords)
        has_1d_lon = any(coord in ds.coords and ds[coord].ndim == 1 for coord in lon_coords)
        
        if has_1d_lat and has_1d_lon:
            return 'structured'
        
        raise ValueError(f"Could not determine grid type for file TODO")     # TODO: get access to file_path to raise error
    
    def _whether_multi_level(self, dims: dict) -> tuple[bool, list[str], int]:
        """Whether the file has a multi-level dimension.
        
        Returns:
        - has_level (bool): Whether the file has level dimensions
        - level_dims (list[str]): List of level dimension names found
        - level_count (int): Total number of level dimensions
        """
        level_dims = ['lev', 'level', 'depth', 'z']
        has_level = any(dim in dims for dim in level_dims)
        level_count = sum(dims.get(dim, 0) for dim in level_dims)
        return has_level, level_dims, level_count
    
    def _has_level_lightweight(self, file_path: Path) -> bool:
        """Lightweight check for level dimensions without opening the full dataset.
        
        This method only reads the dimensions from the NetCDF file header,
        avoiding the expensive operation of loading the full dataset.
        
        Args:
            file_path (Path): Path to the NetCDF file
            
        Returns:
            bool: True if the file has level dimensions, False otherwise
        """
        try:
            # Use xarray's minimal loading to get just dimensions
            with xa.open_dataset(file_path, decode_times=False) as ds:
                dims = dict(ds.sizes)
                has_level, _, _ = self._whether_multi_level(dims)
                return has_level
        except Exception as e:
            self.logger.warning(f"Could not check levels in {file_path}: {e}")
            return False
    
    def _get_file_info(self, file_path: Path) -> dict:
        """Get comprehensive information about a file with caching.
        
        Returns:
        - 'file_size_gb': File size in GB
        - 'dims': Dimensions of the file
        - 'has_level': Whether the file has a level dimension
        - 'level_count': Number of level dimensions
        - 'coords': Coordinate information
        - 'grid_type': Grid type
        - 'estimated_memory_gb': Estimated memory usage in GB
        - 'time_steps': Number of time steps
        """
        # check cache first
        if file_path in self._file_info_cache:
            return self._file_info_cache[file_path]
        
        try:
            ds = xa.open_dataset(file_path, decode_times=False)
            
            # get metadata
            file_size_gb = file_path.stat().st_size / (1024**3) # faster than ds.nbytes
            dims = dict(ds.sizes)    
            has_level, _, level_count = self._whether_multi_level(dims)

            # get coordinate information
            coords_info = {}
            try:
                for coord in ['lon', 'longitude', 'lat', 'latitude']:
                    if coord in ds.coords:
                        coord_data = ds[coord]
                        coords_info[coord] = {
                            'shape': coord_data.shape,
                            'ndim': coord_data.ndim,
                            'size': coord_data.size,
                        }
            except Exception as coord_error:
                self.logger.warning(f"Could not get coordinate info for {file_path}: {coord_error}")
                coords_info = {}
            
            # determine grid type
            try:
                grid_type = self._detect_grid_type(ds, dims)
            except Exception as grid_error:
                self.logger.warning(f"Could not detect grid type for {file_path}: {grid_error}")
                grid_type = 'unknown'
            
            # estimate memory usage required for regridding
            estimated_memory_gb = file_size_gb * 3  # rough but conservative estimate: when regridding will have original, multiple, and maybe an intermediate array in memory
            
            file_info = {
                'file_size_gb': file_size_gb,
                'dims': dims,
                'has_level': has_level,
                'level_count': level_count,
                'coords': coords_info,
                'grid_type': grid_type,
                'estimated_memory_gb': estimated_memory_gb,
                'time_steps': dims.get('time', 1),
            }
            
            # cache the result
            self._file_info_cache[file_path] = file_info
            return file_info
            
        except Exception as e:
            self.logger.warning(f"Could not analyze file {file_path}: {e}")
            error_info = {
                'file_size_gb': 0.0,
                'dims': {},
                'has_level': False,
                'level_count': 0,
                'coords': {},
                'grid_type': 'unknown',
                'estimated_memory_gb': 0.0,
                'time_steps': None,
            }
            # cache the error result too to avoid repeated failures
            self._file_info_cache[file_path] = error_info
            return error_info
    
    def _get_grid_signature(self, file_info: dict) -> str:
        """Generate a unique signature for the grid based on file info to avoid duplicating weights."""
        signature_data = {
            'coords': file_info['coords'],
            'dims': file_info['dims'],
            'grid_type': file_info['grid_type'],
        }
        
        signature_str = str(sorted(signature_data.items()))
        return hashlib.md5(signature_str.encode()).hexdigest()[:12]
    
    def _get_weight_path(self, grid_signature: str) -> Path:
        """Get the path for regrid weights based on grid signature."""
        return self.weight_cache_dir / f"weights_{grid_signature}.nc"   # TODO: make sure this works for directories
    
    def _generate_output_filename(self, input_path: Path, has_level: bool, top_level_only: bool = False) -> str:
        """Generate output filename by modifying the input filename.
        
        Args:
            input_path (Path): Path to the input file
            top_level_only (bool): Whether the file was processed with top_level_only=True
            
        Returns:
            str: Generated output filename
        """
        name = input_path.name
        
        if top_level_only and '_top_level' not in name and has_level:
            # insert _top_level before the extension
            name = name.replace(input_path.suffix, '_top_level' + input_path.suffix)
        
        # insert _regridded before the extension
        return name.replace(input_path.suffix, '_regridded' + input_path.suffix)
    
    def _get_representative_file(self, input_files: list[Path]) -> Optional[Path]:
        """Get a representative file from a list of files for resolution calculation.
        
        Args:
            input_files (list[Path]): List of input files
            
        Returns:
            Optional[Path]: A representative file, or None if no suitable file found
        """
        if not input_files:
            return None
            
        for file_path in input_files:
            try:
                with xa.open_dataset(file_path, decode_times=False) as ds:
                    if 'nominal_resolution' in ds.attrs:    # try to find a file with nominal_resolution attribute
                        return file_path
            except Exception:
                continue
                
        return input_files[0]   # if no file has nominal_resolution, return the first file 
    
    def _calculate_target_resolution(self, input_file: Path) -> tuple[float, float]:
        """Calculate target resolution based on dataset's nominal_resolution attribute.
        
        Args:
            input_file (Path): Path to the input file to analyze
            
        Returns:
            tuple[float, float]: (lon_res, lat_res) in degrees, or (9999.0, 9999.0) if calculation fails
        """
        try:
            with xa.open_dataset(input_file, decode_times=False) as ds:
                # check if nominal_resolution attribute exists
                if 'nominal_resolution' not in ds.attrs:
                    self.logger.warning(f"No 'nominal_resolution' attribute found in {input_file.name}")
                    return (9999.0, 9999.0)
                
                # get native resolution
                native_res = ds.attrs['nominal_resolution']
                self.logger.info(f"Found nominal_resolution: {native_res}")
                
                # calculate target resolution
                target_res = utils.calc_resolution(native_res)
                
                if target_res == 9999.0:
                    self.logger.warning(f"Could not parse nominal_resolution '{native_res}' from {input_file.name}")
                    return (9999.0, 9999.0)
                
                # return same resolution for both lon and lat (regular grid)
                return (target_res, target_res)
                
        except Exception as e:
            self.logger.warning(f"Error calculating target resolution from {input_file.name}: {e}")
            return (9999.0, 9999.0)
    
    def _generate_target_grid_description(self, representative_file: Optional[Path] = None) -> str:
        """Generate CDO target grid description.
        
        Args:
            representative_file (Path, optional): File to use for resolution calculation.
                                                 If None, uses the pipeline's target_resolution.
        """
        # try to calculate target resolution from a representative file
        if representative_file and representative_file.exists():
            lon_res, lat_res = self._calculate_target_resolution(representative_file)
            if lon_res == 9999.0 or lat_res == 9999.0:
                # fall back to pipeline's target resolution
                lon_res, lat_res = self.target_resolution
                if self.verbose:
                    self.console.print(f"[yellow]Using pipeline target resolution: {lon_res}° x {lat_res}°[/yellow]")
            else:
                if self.verbose:
                    self.console.print(f"[green]Calculated target resolution from {representative_file.name}: {lon_res:.3f}° x {lat_res:.3f}° (to 3 decimal places)[/green]")
        elif hasattr(self, '_representative_file') and self._representative_file and self._representative_file.exists():
            # use the pipeline's representative file
            lon_res, lat_res = self._calculate_target_resolution(self._representative_file)
            if lon_res == 9999.0 or lat_res == 9999.0:
                # fall back to pipeline's target resolution
                lon_res, lat_res = self.target_resolution
                if self.verbose:
                    self.console.print(f"[yellow]Using pipeline target resolution: {lon_res}° x {lat_res}°[/yellow]")
            else:
                if self.verbose:
                    self.console.print(f"[green]Calculated target resolution from {self._representative_file.name}: {lon_res}° x {lat_res}°[/green]")
        else:
            # use pipeline's target resolution
            lon_res, lat_res = self.target_resolution
            if self.verbose:
                self.console.print(f"[blue]Using pipeline target resolution: {lon_res}° x {lat_res}°[/blue]")
        
        if self.target_grid == "lonlat":
            # regular lon/lat grid
            xsize = int(360 / lon_res)
            ysize = int(180 / lat_res)
            xfirst = -180 + lon_res / 2 # TODO: check this logic
            yfirst = -90 + lat_res / 2 # TODO: the extent of the grid is not always -180 to 180 and -90 to 90: does this matter? Hard to say, since trying to get everything onto the same grid. Does CDO account for this?
            
            return f"""gridtype = lonlat
xsize = {xsize}
ysize = {ysize}
xfirst = {xfirst}
xinc = {lon_res}
yfirst = {yfirst}
yinc = {lat_res}"""
        
        elif self.target_grid == "gaussian":    # equally spaced along latitude, unequally along longitude
            # Gaussian grid
            ysize = int(180 / lat_res)
            return f"""gridtype = gaussian
ysize = {ysize}"""
        
        else:
            raise ValueError(f"Unsupported target grid type: {self.target_grid}")
    
    def _should_chunk_file(self, file_info: dict) -> bool:
        """Determine if a file should be processed in chunks based on file size, memory usage, and time steps.
        
        Returns (bool): True if the file should be processed in chunks, False otherwise
        """
        if not self.enable_chunking:
            return False
        
        return (
            file_info['file_size_gb'] > self.chunk_size_gb or
            file_info['estimated_memory_gb'] > self.max_memory_gb or
            file_info['time_steps'] > 100
        )
    
    def _chunk_file_by_time(self, file_path: Path, chunk_size: int = 10) -> list[Path]:
        """Split a file into time chunks, saving the rechunked files to the same directory as the original file.
        
        Returns (list[Path]): List of paths to the chunked files
        """
        ds = None
        chunk_files = []
        try:
            ds = xa.open_dataset(file_path, decode_times=False)
            
            # Check if file has time dimension and enough time steps to chunk
            if 'time' not in ds.dims:
                self.logger.warning(f"File {file_path.name} has no 'time' dimension, skipping chunking")
                return [file_path]
            
            time_length = len(ds.time)
            if time_length <= chunk_size:
                if self.verbose:
                    self.logger.debug(f"File {file_path.name} has {time_length} time steps (<= {chunk_size}), skipping chunking")
                return [file_path]
            
            time_chunks = list(range(0, time_length, chunk_size))
            
            # Check if this is a prepared top_level file and extract base name
            is_top_level_file = '_top_level' in file_path.stem
            if is_top_level_file:
                # Remove _top_level from stem to get base name, then add chunk and _top_level
                base_stem = file_path.stem.replace('_top_level', '')
                chunk_stem_template = f"{base_stem}_chunk_{{i:03d}}_top_level"
            else:
                chunk_stem_template = f"{file_path.stem}_chunk_{{i:03d}}"
            
            # Create chunks
            for i, start_idx in enumerate(time_chunks):
                end_idx = min(start_idx + chunk_size, time_length)
                ds_chunk = ds.isel(time=slice(start_idx, end_idx))
                
                # Verify chunk has data before writing
                if ds_chunk.sizes.get('time', 0) == 0:
                    self.logger.warning(f"Skipping empty chunk {i} for {file_path.name}")
                    continue
                
                chunk_path = file_path.parent / f"{chunk_stem_template.format(i=i)}{file_path.suffix}"
                
                # Write chunk file
                try:
                    ds_chunk.to_netcdf(chunk_path)
                    
                    # Verify chunk file was created and is not empty
                    if not chunk_path.exists() or chunk_path.stat().st_size == 0:
                        self.logger.error(f"Chunk file {chunk_path.name} is empty or was not created")
                        if chunk_path.exists():
                            chunk_path.unlink()
                        continue
                    
                    # Track the created chunk file for cleanup
                    self._track_created_file(chunk_path)
                    chunk_files.append(chunk_path)
                    
                except Exception as chunk_error:
                    self.logger.error(f"Failed to write chunk {i} for {file_path.name}: {chunk_error}")
                    # Clean up failed chunk file if it was partially created
                    if chunk_path.exists():
                        try:
                            chunk_path.unlink()
                        except Exception:
                            pass
                    raise  # Re-raise to trigger cleanup
            
            # If no chunks were created successfully, return original file
            if not chunk_files:
                self.logger.warning(f"No chunks created for {file_path.name}, returning original file")
                return [file_path]
            
            return chunk_files
            
        except Exception as e:
            self.logger.error(f"Failed to chunk file {file_path}: {e}")
            self.logger.error(f"Full traceback: {traceback.format_exc()}")
            
            # Clean up any chunk files that were created before the error
            for chunk_file in chunk_files:
                try:
                    if chunk_file.exists() and chunk_file != file_path:
                        self.logger.warning(f"Cleaning up incomplete chunk file: {chunk_file.name}")
                        chunk_file.unlink()
                except Exception as cleanup_error:
                    self.logger.error(f"Failed to clean up chunk file {chunk_file.name}: {cleanup_error}")
            
            return [file_path]
        finally:
            # Always close the dataset if it was opened
            if ds is not None:
                try:
                    ds.close()
                except Exception:
                    pass
    
    def _prepare_file_for_regridding(self, file_path: Path) -> Path:
        """
        Prepare a file for regridding by selecting top level if needed.
        
        Returns (Path): Path to prepared file.
        """
        if not self.top_level_only:
            return file_path
        
        try:
            # check if file has depth/level dimensions
            # if has top_level in fp, just return the file
            if '_top_level' in file_path.stem:
                return file_path
            
            ds = xa.open_dataset(file_path, decode_times=False)
            has_level, level_dims, level_count = self._whether_multi_level(dict(ds.sizes))
            
            if not isinstance(level_dims, list):
                self.logger.error(f"level_dims is not a list: {type(level_dims)} = {level_dims}")
                return file_path
            
            if not has_level or level_count == 0:
                return file_path
            
            # if multi-level, select top level. TODO: add seafloor fetching logic
            for dim in level_dims:
                if dim in ds.dims:
                    ds = ds.isel({dim: 0})  # get the top level
                    break
            
            # save prepared file to same directory as original file
            prepared_path = file_path.parent / f"{file_path.stem}_top_level{file_path.suffix}"  # TODO: is this going to get confusing?
            ds.to_netcdf(prepared_path)
            
            # track the created file for cleanup
            self._track_created_file(prepared_path)
            
            if self.verbose:
                self.console.print(f"[cyan]Prepared file (top level): {prepared_path.name}[/cyan]")
                self.logger.info(f"Prepared file (top level): {prepared_path.name}")
            
            return prepared_path
            
        except Exception as e:
            self.logger.warning(f"Could not prepare file {file_path}: {e}")
            self.logger.error(f"Full traceback: {traceback.format_exc()}")
            return file_path
    
    def _regrid_chunked_file(
        self,
        input_path: Path,
        output_path: Path,
        grid_signature: str,
        grid_type: str,
        chunk_files: list[Path],
    ) -> bool:
        """Regrid a file that has been split into chunks.
        
        Args:
        - input_path (Path): Path to the input file
        - output_path (Path): Path to the output file
        - grid_signature (str): Unique signature for the grid
        - grid_type (str): Type of grid
        - chunk_files (list[Path]): List of paths to the chunked files
        
        Returns (bool): True if successful, False otherwise
        """
        try:
            weight_path = self._get_weight_path(grid_signature)
            grid_desc = self._generate_target_grid_description(input_path)
            
            with tempfile.TemporaryDirectory() as tmpdir:
                tmpdir = Path(tmpdir)
                grid_file = tmpdir / "target_grid.txt"
                
                # write target grid description to temporary directory
                with open(grid_file, 'w') as f:
                    f.write(grid_desc)
                
                # process each chunk
                chunk_outputs = []
                for i, chunk_file in enumerate(chunk_files):
                    chunk_output = tmpdir / f"chunk_{i:03d}.nc"
                    
                    if weight_path.exists():
                        self._regrid_with_weights(chunk_file, chunk_output, grid_file, weight_path)  # use existing weights to regrid
                    else:
                        self._regrid_without_weights(chunk_file, chunk_output, grid_file, grid_type)  # generate weights and regrid
                        
                        # save weights for future use
                        if i == 0:  # only save weights from first chunk (chunked by time, therefore spatially identical)
                            # TODO: any issues if uneven number of time steps between chunks?
                            self._save_weights(chunk_file, weight_path, grid_file)
                    
                    chunk_outputs.append(chunk_output)
                    self.stats['chunks_processed'] += 1
                
                # combine chunks into single file
                if len(chunk_outputs) > 1:
                    self._combine_chunks(chunk_outputs, output_path)
                else:
                    shutil.copy2(chunk_outputs[0], output_path)
                
                # clean up chunk files
                for chunk_file in chunk_files:
                    if chunk_file != input_path and chunk_file.exists():
                        try:
                            chunk_file.unlink()
                            self.logger.debug(f"Cleaned up chunk file: {chunk_file.name}")
                        except Exception as cleanup_error:
                            self.logger.warning(f"Failed to clean up chunk file {chunk_file.name}: {cleanup_error}")
                
                return True
                
        except Exception as e:
            self.logger.error(f"Failed to regrid chunked file {input_path}: {e}")
            self.logger.error(f"Full traceback: {traceback.format_exc()}")
            
            # Clean up chunk files even if regridding failed
            for chunk_file in chunk_files:
                if chunk_file != input_path and chunk_file.exists():
                    try:
                        self.logger.warning(f"Cleaning up chunk file after error: {chunk_file.name}")
                        chunk_file.unlink()
                    except Exception as cleanup_error:
                        self.logger.error(f"Failed to clean up chunk file {chunk_file.name}: {cleanup_error}")
            
            return False

    def _combine_chunks(self, chunk_outputs: list[Path], output_path: Path):
        """Combine regridded chunks into a single file.
        
        Args:
        - chunk_outputs (list[Path]): List of paths to the regridded chunks
        - output_path (Path): Path to the output file
        
        Returns (bool): True if successful, False otherwise
        """
        try:
            # load all chunks   # TODO: xa.open_mfdataset complains about dask not being installed
            datasets = [xa.open_dataset(chunk) for chunk in chunk_outputs]
            combined = xa.concat(datasets, dim='time')  # combine along time dimension
            combined.to_netcdf(output_path)
            # close datasets to free memory
            for ds in datasets:
                ds.close()
                
        except Exception as e:
            self.logger.error(f"Failed to combine chunks: {e}")
            self.logger.error(f"Full traceback: {traceback.format_exc()}")
            return False
    
    def _regrid_with_weights(self, input_path: Path, output_path: Path, grid_file: Path, weight_path: Path):
        """Regrid using existing weights.
        
        Args:
        - input_path (Path): Path to the input file
        - output_path (Path): Path to the output file
        - grid_file (Path): Path to the grid file
        - weight_path (Path): Path to the weight file
        """
        try:
            self.cdo.remap(
                str(grid_file),
                str(weight_path),
                input=str(input_path),
                output=str(output_path),
            )
            return True
        except Exception as e:
            self.logger.error(f"Failed to regrid {input_path} with weights: {e}")
            self.logger.error(f"Full traceback: {traceback.format_exc()}")
            return False
    
    def _regrid_without_weights(self, input_path: Path, output_path: Path, grid_file: Path, grid_type: str):
        """Regrid without existing weights using appropriate method. Conservative remapping is used for all grid types to preserve mass/volume."""
        try:
            self.cdo.remapcon(
                str(grid_file),
                input=str(input_path),
                output=str(output_path),
            )
            return True
        except Exception as e:
            self.logger.error(f"Failed to regrid {input_path} of type {grid_type} without weights: {e}")
            self.logger.error(f"Full traceback: {traceback.format_exc()}")
            return False
    
    def _regrid_single_file(
        self,
        input_path: Path,
        output_path: Path,
        grid_signature: str,
        grid_type: str,
        force_regenerate_weights: bool = False,
    ) -> bool:
        """
        Regrid a single file using CDO with grid-type-specific handling.
        
        Args:
        - input_path (Path): Path to the input file
        - output_path (Path): Path to the output file
        - grid_signature (str): Unique signature for the grid
        - grid_type (str): Type of grid
        - force_regenerate_weights (bool): Whether to force regeneration of weights
        
        Returns (bool): True if successful, False otherwise
        """
        try:
            # get weight file path
            weight_path = self._get_weight_path(grid_signature)
            
            # generate target grid description
            grid_desc = self._generate_target_grid_description(input_path)
            
            with tempfile.TemporaryDirectory() as tmpdir:
                tmpdir = Path(tmpdir)
                grid_file = tmpdir / "target_grid.txt"
                
                # write grid description
                with open(grid_file, 'w') as f:
                    f.write(grid_desc)
                
                # check to see whether we should reuse weights
                if weight_path.exists() and not force_regenerate_weights:
                    self.console.print(f"[green]Reusing weights: {weight_path.name}[/green]") if self.verbose else None
                    self.stats['weights_reused'] += 1
                    # use existing weights
                    self._regrid_with_weights(input_path, output_path, grid_file, weight_path)
                else:
                    # generate new weights
                    self.console.print(f"[yellow]Generating new weights: {weight_path.name}[/yellow]") if self.verbose else None
                    # regrid without weights
                    self._regrid_without_weights(input_path, output_path, grid_file, grid_type)
                    # save weights for future use
                    self._save_weights(input_path, weight_path, grid_file)
                    self.stats['weights_generated'] += 1
                
                # verify output file exists and is not empty
                if output_path.exists() and output_path.stat().st_size > 0:
                    return True
                else:
                    self.logger.error(f"Output file is empty or missing: {output_path}")
                    self.logger.error(f"Full traceback: {traceback.format_exc()}")
                    return False
                    
        except Exception as e:
            self.logger.error(f"Failed to regrid {input_path}: {e}")
            self.logger.error(f"Full traceback: {traceback.format_exc()}")
            return False
    
    def _save_weights(self, input_path: Path, weight_path: Path, grid_file: Path) -> None:
        """Save regrid weights for future reuse with grid-type-specific method.
        
        Args:
        - input_path (Path): Path to the input file
        - weight_path (Path): Path to the weight file
        - grid_file (Path): Path to the grid file
        
        Returns (None): None
        """
        try:
            with tempfile.TemporaryDirectory() as tmpdir:
                tmpdir = Path(tmpdir)
                temp_weights = tmpdir / "temp_weights.nc"
                
                self.cdo.gencon(
                    str(grid_file),
                    input=str(input_path),
                    output=str(temp_weights),
                )
                    
                # copy from tmpdir to weight_path, preserving dataset attributes
                shutil.copy2(temp_weights, weight_path)
                return True
        except Exception as e:
            self.logger.warning(f"Could not save weights: {e}")
            self.logger.error(f"Full traceback: {traceback.format_exc()}")
            return False
    
    def regrid_file(
        self,
        input_path: Path,
        output_path: Optional[Path] = None,
        force_regenerate_weights: bool = False,
        overwrite: bool = False,
        ui: Optional[RegridProgressUI] = None,
    ) -> bool:
        """
        Regrid a single file with comprehensive grid type support and memory optimization.
        
        Args:
        - input_path (Path): Path to the input file
        - output_path (Path): Path to the output file
        - force_regenerate_weights (bool): Whether to force regeneration of weights
        - overwrite (bool): If True, overwrite existing output files
        
        Returns (bool): True if successful, False otherwise
        """
        if not input_path.exists():
            self.logger.error(f"Input file does not exist: {input_path}")
            self.logger.error(f"Full traceback: {traceback.format_exc()}")
            return False
        
        file_info = self._get_file_info(input_path)
        
        if ui:
            ui.start_file_processing(input_path, file_info)  # start UI progress if provided
        
        if output_path is None:
            output_filename = self._generate_output_filename(input_path, file_info['has_level'], self.top_level_only)
            output_path = input_path.parent / output_filename  # determine output path
        
        if output_path.exists():
            if overwrite:   # handle overwrite logic
                if self.verbose:
                    self.console.print(f"[yellow]Overwriting existing file: {output_path.name}[/yellow]")
                try:
                    output_path.unlink()
                except Exception as e:
                    self.logger.error(f"Failed to remove existing file {output_path}: {e}")
                    if ui:
                        ui.complete_file(input_path, success=False, message=f"Failed to remove existing file: {e}")
                    return False
            else:
                if self.verbose:
                    self.console.print(f"[yellow]Skipping existing file: {output_path.name}[/yellow]")
                if ui:
                    ui.skip_file(input_path, "File already exists")
                return True
        
        grid_type = file_info['grid_type']
        
        if ui:   # prepare file for regridding (select top level if needed)
            ui.update_file_progress(input_path, 10, "Preparing file")
        prepared_path = self._prepare_file_for_regridding(input_path)
        
        self.stats['grid_types'][grid_type] += 1  # update statistics
        
        if self.verbose:
            self.console.print(f"[blue]Grid type: {grid_type}[/blue]")
            self.console.print(f"[blue]File size: {file_info['file_size_gb']:.2f} GB[/blue]")
            if file_info['has_level']:
                self.console.print(f"[blue]Levels: {file_info['level_count']}[/blue]")
        
        should_chunk = self._should_chunk_file(file_info)
        
        if should_chunk and self.verbose:
            self.console.print(f"[yellow]Large file detected ({file_info['file_size_gb']:.2f} GB), using chunked processing[/yellow]")
            if ui:
                ui.update_file_progress(input_path, 20, "Chunking large file")
    
        
        try:
            grid_signature = self._get_grid_signature(file_info)
            
            if should_chunk:   # check if we should chunk the file, if so, process in chunks
                if ui:
                    ui.update_file_progress(input_path, 30, "Creating chunks")
                chunk_files = self._chunk_file_by_time(prepared_path)
                if ui:
                    ui.update_file_progress(input_path, 50, "Regridding chunks")
                success = self._regrid_chunked_file(
                    prepared_path,
                    output_path,
                    grid_signature,
                    grid_type,
                    chunk_files,
                )
            else:   # process without chunking
                if ui:
                    ui.update_file_progress(input_path, 30, "Regridding file")
                success = self._regrid_single_file(
                    prepared_path,
                    output_path,
                    grid_signature,
                    grid_type,
                    force_regenerate_weights,
                )
            
            if success:
                self.stats['files_processed'] += 1
                self.stats['total_size_gb'] += file_info['file_size_gb']
                
                # update memory monitoring
                if self.memory_monitor:
                    self.memory_monitor.update_peak()
                    self.stats['memory_peak_gb'] = self.memory_monitor.get_peak_memory_gb()
                
                if ui:   # complete UI progress
                    ui.complete_file(input_path, success=True)
            else:
                if ui:
                    ui.complete_file(input_path, success=False, message="Regridding failed")
            
            if prepared_path != input_path and prepared_path.exists():
                prepared_path.unlink()  # clean up prepared file if it was created
            
            return success
            
        except Exception as e:
            self.logger.error(f"Error processing {input_path}: {e}")
            self.logger.error(f"Full traceback: {traceback.format_exc()}")
            self.stats['errors'] += 1
            if ui:
                ui.complete_file(input_path, success=False, message=f"Error: {str(e)}")
            return False
    
    def regrid_batch(
        self,
        input_files: list[Path],
        output_dir: Optional[Path] = None,
        group_by_directory: bool = True,
        overwrite: bool = False,
        use_ui: bool = True,
    ) -> dict[str, list[Path]]:
        """Regrid a batch of files efficiently with optional parallel processing.
        
        Args:
        - input_files (list[Path]): List of input files to regrid
        - output_dir (Path): Output directory. If None, outputs go to same directory as input
        - group_by_directory (bool): Group files by directory to maximize weight reuse
        - overwrite (bool): If True, overwrite existing output files
        
        Returns (dict[str, list[Path]]): Dictionary mapping status to list of file paths
        
        Note: has two child functions, _regrid_batch_sequential and _regrid_batch_parallel depending on number of files, and whether parallel processing is enabled and is successful.
        """
        results = {
            'successful': [],
            'failed': [],
            'skipped': [],
        }
        
        if isinstance(input_files, Path):
            input_files = [input_files]
            self.console.print(f"[yellow]Input is a single file, will be processed sequentially[/yellow]") if self.verbose else None
        
        # determine representative file for resolution calculation
        representative_file = self._get_representative_file(input_files)
        if representative_file and self.verbose:
            self.console.print(f"[blue]Using representative file for resolution calculation: {representative_file.name}[/blue]")
            
        # clean up problematic files (_top_level and _chunk_) first
        input_files = self._cleanup_problematic_files(input_files)
        
        if overwrite:
            # unlink any files with 'regridded' in the name in order to reprocess them
            for file in input_files:
                if 'regridded' in file.name:
                    if self.verbose:
                        self.console.print(f"[yellow]Removing existing regridded file: {file.name}[/yellow]")
                    file.unlink()
            # prune any files with 'regridded' in the name to avoid processing them twice
            input_files = [
                file for file in input_files
                if 'regridded' not in file.name and not file.suffix.endswith('.part')
            ]   # TODO: use _prune_regridded function instead (or remove it)
            
            # also check for and remove existing output files that would be created from input files
            if self.verbose:
                self.console.print(f"[blue]Checking for existing output files to remove (overwrite=True)[/blue]")
            for file in input_files[:]:  # use slice copy to avoid modifying list while iterating
                # generate the expected output filename using lightweight check
                has_level = self._has_level_lightweight(file)
                output_filename = self._generate_output_filename(file, has_level, self.top_level_only)
                if output_dir:
                    expected_output = output_dir / output_filename
                else:
                    expected_output = file.parent / output_filename
                
                if expected_output.exists():
                    if self.verbose:
                        self.console.print(f"[yellow]Removing existing output file: {expected_output.name}[/yellow]")
                    expected_output.unlink()
        else:
            # when overwrite=False, just prune file names with 'regridded' in the name from processing list but don't delete them
            if self.verbose:
                regridded_files = [file for file in input_files if 'regridded' in file.name]
                if regridded_files:
                    self.console.print(f"[blue]Skipping {len(regridded_files)} existing regridded files (overwrite=False)[/blue]")
            input_files = [
                file for file in input_files
                if 'regridded' not in file.name and not file.suffix.endswith('.part')
            ]
        
        self.start_time = fileops.print_timestamp(self.console, "START")
        
        if not self.enable_parallel or len(input_files) < 2 or self.max_workers in [None, 1]:
            # process sequentially
            results = self._regrid_batch_sequential(input_files, output_dir, group_by_directory, overwrite, use_ui)
            self.end_time = fileops.print_timestamp(self.console, "END")
            return results
        
        try:
            # initialize compact UI for parallel processing
            ui = None
            if use_ui and self.verbose:
                ui = BatchRegridUI(input_files, max_workers=self.max_workers, verbose=self.verbose)
                ui.__enter__()
            
            # process files in parallel (individual file processing)
            if self.verbose:
                self.console.print(f"[green]Processing {len(input_files)} files in parallel with {self.max_workers} workers[/green]")
            
            with ProcessPoolExecutor(max_workers=self.max_workers) as executor:
                futures = []
                
                # submit each file individually for parallel processing
                for file_path in input_files:
                    # submit individual file for processing
                    future = executor.submit(
                        _process_single_file_standalone,
                        file_path,
                        output_dir,
                        self.target_resolution,
                        self.target_grid,
                        self.weight_cache_dir,
                        self.top_level_only,
                        self.max_memory_gb,
                        self.chunk_size_gb,
                        self.enable_chunking,
                        overwrite,
                        representative_file,
                        self.verbose,
                    )
                    futures.append(future)
                
                # collect results and combine statistics
                completed = 0
                combined_stats = {
                    'files_processed': 0,
                    'weights_reused': 0,
                    'weights_generated': 0,
                    'chunks_processed': 0,
                    'errors': 0,
                    'total_size_gb': 0.0,
                    'memory_peak_gb': 0.0,
                    'grid_types': {}
                }
                
                for future in futures:
                    file_result = future.result()
                    completed += 1
                    
                    # update UI with result
                    if ui:
                        ui.update_file_result(file_result['file_path'], file_result)
                    
                    # combine statistics from worker
                    if 'stats' in file_result:
                        worker_stats = file_result['stats']
                        combined_stats['files_processed'] += worker_stats.get('files_processed', 0)
                        combined_stats['weights_reused'] += worker_stats.get('weights_reused', 0)
                        combined_stats['weights_generated'] += worker_stats.get('weights_generated', 0)
                        combined_stats['chunks_processed'] += worker_stats.get('chunks_processed', 0)
                        combined_stats['errors'] += worker_stats.get('errors', 0)
                        combined_stats['total_size_gb'] += worker_stats.get('total_size_gb', 0.0)
                        combined_stats['memory_peak_gb'] = max(combined_stats['memory_peak_gb'], worker_stats.get('memory_peak_gb', 0.0))
                        
                        # combine grid types
                        for grid_type, count in worker_stats.get('grid_types', {}).items():
                            if grid_type not in combined_stats['grid_types']:
                                combined_stats['grid_types'][grid_type] = 0
                            combined_stats['grid_types'][grid_type] += count
                    
                    if file_result['success']:
                        if file_result.get('skipped', False):
                            results['skipped'].append(file_result['file_path'])
                        else:
                            results['successful'].append(file_result['file_path'])
                    else:
                        results['failed'].append(file_result['file_path'])
                
                # update main pipeline statistics
                self.stats.update(combined_stats)
        except Exception as e:
            self.logger.error(f"Error processing batch in parallel: {e}")
            self.logger.error(f"Full traceback: {traceback.format_exc()}")
            self.logger.warning(f"Falling back to sequential processing")
            results = self._regrid_batch_sequential(input_files, output_dir, group_by_directory, overwrite, use_ui)
            self.end_time = fileops.print_timestamp(self.console, "END")
            return results
        finally:
            if ui:
                # Add timing to stats
                self.stats['processing_time'] = fileops.format_processing_time(fileops.get_processing_time(self.start_time, self.end_time))
                ui._update_stats(self.stats)
                ui.print_summary()
                ui.__exit__(None, None, None)
            self.end_time = fileops.print_timestamp(self.console, "END")
        
        return results
    
    def _regrid_batch_sequential(
        self,
        input_files: list[Path],
        output_dir: Optional[Path] = None,
        group_by_directory: bool = True,
        overwrite: bool = False,
        use_ui: bool = True,
    ) -> dict[str, list[Path]]:
        """Sequential batch processing fallback. Used if parallel processing fails or is not enabled, if there is only one file, or if the number of workers is not specified or is set to 1.
        
        Args:
        - input_files (list[Path]): List of input files to regrid
        - output_dir (Path): Output directory. If None, outputs go to same directory as input
        - group_by_directory (bool): Group files by directory to maximize weight reuse
        
        Returns (dict[str, list[Path]]): Dictionary mapping status to list of file paths
        
        Note: has one regridding child function, regrid_file, for processing a single file.
        """
        results = {
            'successful': [],
            'failed': [],
            'skipped': [],
        }
        
        # clean up problematic files (_top_level and _chunk_) first
        input_files = self._cleanup_problematic_files(input_files)
        
        # initialize UI if requested
        ui = None
        if use_ui and self.verbose:
            ui = RegridProgressUI(input_files, verbose=self.verbose)
            ui.__enter__()
        
        # group files by directory if requested
        if group_by_directory:
            file_groups = self._group_files_by_directory(input_files)
        else:
            file_groups = {'all': input_files}
        
        # process each group
        for group_name, files in file_groups.items():
            if self.verbose:
                self.console.print(f"\n[blue]Processing group: {group_name}[/blue]")
                self.console.print(f"[blue]Files in group: {len(files)}[/blue]")
            
            # process files in group
            for file_path in files:
                try:
                    # Use lightweight check for has_level to avoid expensive full file analysis
                    has_level = self._has_level_lightweight(file_path)
                    
                    # determine output path
                    if output_dir:
                        output_filename = self._generate_output_filename(file_path, has_level, self.top_level_only)
                        output_path = output_dir / output_filename
                    else:
                        output_filename = self._generate_output_filename(file_path, has_level, self.top_level_only)
                        output_path = file_path.parent / output_filename
                    
                    # check if output already exists
                    if output_path.exists():
                        results['skipped'].append(file_path)
                        if self.verbose:
                            self.console.print(f"[yellow]Skipping (exists): {file_path.name}[/yellow]")
                        continue
                    
                    # regrid file
                    success = self.regrid_file(file_path, output_path, overwrite=overwrite, ui=ui)
                    
                    if success:
                        results['successful'].append(file_path)
                        if self.verbose:
                            self.console.print(f"[green]Success: {file_path.name}[/green]")
                    else:
                        results['failed'].append(file_path)
                        if self.verbose:
                            self.console.print(f"[red]Failed: {file_path.name}[/red]")
                        
                except Exception as e:
                    self.logger.error(f"Error processing {file_path}: {e}")
                    self.logger.error(f"Full traceback: {traceback.format_exc()}")
                    results['failed'].append(file_path)
                    if ui:
                        ui.complete_file(file_path, success=False, message=f"Error: {str(e)}")
        
        if ui:
            self.stats['processing_time'] = fileops.format_processing_time(fileops.get_processing_time(self.start_time, self.end_time))
            ui._update_stats(self.stats)
            ui.print_summary()
            ui.__exit__(None, None, None)
        
        return results
    
    
    def _group_files_by_directory(self, files: list[Path]) -> dict[str, list[Path]]:
        """Group files by their parent directory.
        
        Args:
        - files (list[Path]): List of files to group
        
        Returns (dict[str, list[Path]]): Dictionary mapping directory to list of file paths
        """
        groups = {}
        for file_path in files:
            parent_dir = str(file_path.parent)
            if parent_dir not in groups:
                groups[parent_dir] = []
            groups[parent_dir].append(file_path)
        return groups
    
    def print_statistics(self):
        """Print comprehensive processing statistics."""
        table = Table(show_header=True, header_style="bold cyan")
        table.add_column("Metric", style="bold")
        table.add_column("Value", style="bold")
        
        table.add_row("Files Processed", str(self.stats['files_processed']))
        table.add_row("Weights Reused", str(self.stats['weights_reused']))
        table.add_row("Weights Generated", str(self.stats['weights_generated']))
        table.add_row("Chunks Processed", str(self.stats['chunks_processed']))
        table.add_row("Errors", str(self.stats['errors']))
        table.add_row("Total Size (GB)", f"{self.stats['total_size_gb']:.2f}")
        table.add_row("Memory Peak (GB)", f"{self.stats['memory_peak_gb']:.2f}")
        
        # add grid type statistics
        table.add_row("", "")  # Empty row
        table.add_row("Grid Types", "")
        for grid_type, count in self.stats['grid_types'].items():
            if count > 0:
                table.add_row(f"  {grid_type}", str(count))
        
        self.console.print(Panel(table, title="[cyan]CDO Regridding Statistics[/cyan]", border_style="cyan"))
    
    def cleanup_weight_files(self, confirm: bool = True):
        """Delete all cached weight files on demand.
        
        Args:
        - confirm (bool): Require user confirmation before deleting files (default: True)
        # TODO: add confirmation option to main function?
        Returns (None): None
        """
        try:
            weight_files = list(self.weight_cache_dir.glob("weights_*.nc"))
            if not weight_files:
                if self.verbose:
                    self.console.print("[yellow]No weight files to clean up.[/yellow]")
                return

            if confirm:
                from rich.prompt import Confirm
                proceed = Confirm.ask(
                    f"[red]Are you sure you want to delete all {len(weight_files)} cached weight files in {self.weight_cache_dir}?[/red]"
                )
                if not proceed:
                    if self.verbose:
                        self.console.print("[yellow]Cleanup cancelled by user.[/yellow]")
                    return

            for weight_file in weight_files:
                try:
                    weight_file.unlink()
                    if self.verbose:
                        self.console.print(f"[cyan]Cleaned up {weight_file.name}[/cyan]")
                except Exception as e:
                    self.logger.error(f"Error deleting weight file {weight_file}: {e}")
            if self.verbose:
                self.console.print(f"[green]Cleaned up {len(weight_files)} weight file(s).[/green]")
        except Exception as e:
            self.logger.error(f"Error cleaning up weights: {e}")
            self.logger.error(f"Full traceback: {traceback.format_exc()}")


class MemoryMonitor:
    """Monitor memory usage during processing."""
    
    def __init__(self):
        self.peak_memory_gb = 0.0
    
    def get_memory_usage_gb(self) -> float:
        """Get current memory usage in GB."""
        process = psutil.Process()
        return process.memory_info().rss / (1024**3)
    
    def update_peak(self):
        """Update peak memory usage."""
        current = self.get_memory_usage_gb()
        if current > self.peak_memory_gb:
            self.peak_memory_gb = current
    
    def get_peak_memory_gb(self) -> float:
        """Get peak memory usage in GB."""
        return self.peak_memory_gb


# ================================
# Convenience functions
# ================================

def regrid_single_file(
    input_path: Path,
    output_path: Optional[Path] = None,
    target_resolution: tuple[float, float] = (1.0, 1.0),
    top_level_only: bool = True,
    verbose: bool = True,
    cleanup_weights: bool = False,
    overwrite: bool = False,
    use_ui: bool = True,
) -> bool:
    """
    Convenience function to regrid a single file.
    
    Args:
    - input_path (Path): Path to input file
    - output_path (Path): Path to output file
    - target_resolution (tuple): Target resolution as (lon_res, lat_res)
    - top_level_only (bool): Only process top level for multi-level files
    - verbose (bool): Enable verbose output
    - cleanup_weights (bool): Clean up weights after processing
    - overwrite (bool): If True, overwrite existing output files
    
    Returns (bool): True if successful, False otherwise
    """
    pipeline = CDORegridPipeline(
        target_resolution=target_resolution,
        top_level_only=top_level_only,
        verbose=verbose,
        cleanup_weights=cleanup_weights,
    )
    
    # Initialize UI if requested
    ui = None
    if use_ui and verbose:
        ui = RegridProgressUI([input_path], verbose=verbose)
        ui.__enter__()
    
    try:
        return pipeline.regrid_file(input_path, output_path, overwrite=overwrite, ui=ui)
    finally:
        if ui:
            # Add timing to stats
            pipeline.stats['processing_time'] = pipeline.format_processing_time()
            ui._update_stats(pipeline.stats)
            ui.print_summary()
            ui.__exit__(None, None, None)


def regrid_directory(
    input_dir: Path,
    include_subdirectories: bool = False,
    output_dir: Optional[Path] = None,
    target_resolution: tuple[float, float] = None,
    file_pattern: str = "*.nc",
    top_level_only: bool = True,
    verbose: bool = True,
    max_workers: Optional[int] = 4,
    enable_parallel: bool = True,
    overwrite: bool = False,
    use_ui: bool = True,
) -> dict[str, list[Path]]:
    """
    Convenience function to regrid all files in a directory.
    
    Args:
    - input_dir (Path): Input directory containing NetCDF files
    - output_dir (Path): Output directory for regridded files
    - target_resolution (tuple): Target resolution as (lon_res, lat_res)
    - file_pattern (str): File pattern to match (e.g., "*.nc", "*.nc4")
    - top_level_only (bool): Only process top level for multi-level files
    - verbose (bool): Enable verbose output
    - max_workers (int): Maximum number of parallel workers
    - enable_parallel (bool): Enable parallel processing
    - overwrite (bool): If True, overwrite existing output files
    
    Returns (dict[str, list[Path]]): Dictionary mapping status to list of file paths
    """
    # find all matching files
    if include_subdirectories:
        input_files = list(input_dir.rglob(file_pattern))
    else:
        input_files = list(input_dir.glob(file_pattern))
    
    if not input_files:
        print(f"No files found matching pattern '{file_pattern}' in {input_dir}")
        return {'successful': [], 'failed': [], 'skipped': []}
    
    # create pipeline
    pipeline = CDORegridPipeline(
        target_resolution=target_resolution,
        top_level_only=top_level_only,
        verbose=verbose,
        max_workers=max_workers,
        enable_parallel=enable_parallel,
    )
    
    # process files
    results = pipeline.regrid_batch(input_files, output_dir, overwrite=overwrite, use_ui=use_ui)
    # print statistics
    pipeline.print_statistics()
    
    return results


def regrid_large_files(
    input_files: list[Path],
    output_dir: Optional[Path] = None,
    target_resolution: tuple[float, float] = (1.0, 1.0),
    chunk_size_gb: float = 2.0,
    max_memory_gb: float = 8.0,
    verbose: bool = True,
    overwrite: bool = False,
) -> dict[str, list[Path]]:
    """
    Convenience function for regridding large files with memory optimization.
    
    Args:
    - input_files (list[Path]): List of input files to regrid
    - output_dir (Path): Output directory for regridded files
    - target_resolution (tuple): Target resolution as (lon_res, lat_res)
    - chunk_size_gb (float): Maximum chunk size in GB
    - max_memory_gb (float): Maximum memory usage in GB
    - verbose (bool): Enable verbose output
    - overwrite (bool): If True, overwrite existing output files
    
    Returns (dict[str, list[Path]]): Dictionary mapping status to list of file paths
    """
    pipeline = CDORegridPipeline(
        target_resolution=target_resolution,
        chunk_size_gb=chunk_size_gb,
        max_memory_gb=max_memory_gb,
        verbose=verbose,
        enable_chunking=True,
        memory_monitoring=True,
    )
    
    results = pipeline.regrid_batch(input_files, output_dir, overwrite=overwrite)
    pipeline.print_statistics()
    
    return results


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="CDO-based NetCDF regridding pipeline")
    parser.add_argument("input", type=Path, help="Input file or directory")
    parser.add_argument("-o", "--output", type=Path, help="Output file or directory")
    parser.add_argument("-r", "--resolution", nargs=2, type=float, default=[1.0, 1.0],
                       help="Target resolution (lon_res lat_res)")
    parser.add_argument("-p", "--pattern", default="*.nc", help="File pattern (for directories)")
    parser.add_argument("--include-subdirectories", action="store_true", default=True, help="Include subdirectories")
    parser.add_argument("--top-level-only", action="store_true", default=True,
                       help="Only process top level for multi-level files")
    parser.add_argument("-v", "--verbose", action="store_true", default=True, help="Verbose output")
    parser.add_argument("--quiet", action="store_true", help="Disable verbose output")
    parser.add_argument("-w", "--max-workers", default=4, type=int, help="Maximum parallel workers")
    parser.add_argument("--chunk-size-gb", type=float, default=2.0,
                       help="Maximum chunk size in GB")
    parser.add_argument("--max-memory-gb", default=8.0, type=float, help="Maximum memory usage in GB")
    parser.add_argument("--no-parallel", action="store_true", default=False, help="Disable parallel processing")
    parser.add_argument("--no-chunking", action="store_true", default=False, help="Disable chunked processing")
    parser.add_argument("--use-ui", action="store_true", default=True, help="Use UI for processing")
    # parser.add_argument("--cleanup", action="store_true", help="Clean up problematic files (*_top_level, *_chunk_*) before processing")
    parser.add_argument("--unlink-unprocessed", action="store_true", default=False, help="Unlink unprocessed files after processing")
    
    args = parser.parse_args()
    
    # handle verbose/quiet logic
    verbose = args.verbose and not args.quiet
    
    # # handle cleanup if requested
    # if args.cleanup:
    #     if args.input.is_file():
    #         # clean up in the same directory as the file
    #         cleaned_count = cleanup_problematic_files(args.input.parent, verbose=verbose)
    #     else:
    #         # clean up in the directory
    #         cleaned_count = cleanup_problematic_files(args.input, verbose=verbose)
        
    #     if cleaned_count == 0:
    #         print("No problematic files found to clean up.")
    #     else:
    #         print(f"Cleaned up {cleaned_count} problematic files.")
        
    #     # exit after cleanup
    #     exit(0)
    
    # determine if input is file or directory
    if args.input.is_file():
        # single file processing
        success = regrid_single_file(
            input_path=args.input,
            output_path=args.output,
            target_resolution=tuple(args.resolution),
            top_level_only=args.top_level_only,
            verbose=verbose,
            use_ui=args.use_ui,
        )
        
        if success:
            print("Regridding successful!")
        else:
            print("Regridding failed!")
    else:
        # directory processing
        results = regrid_directory(
            input_dir=args.input,
            output_dir=args.output,
            include_subdirectories=args.include_subdirectories,
            target_resolution=tuple(args.resolution),
            file_pattern=args.pattern,
            top_level_only=args.top_level_only,
            verbose=verbose,
            max_workers=args.max_workers,
            enable_parallel=not args.no_parallel,
            use_ui=True,
        )
        
        # print results
        console = Console()
        console.print(f"\n[green]Successful: {len(results['successful'])}[/green]")
        console.print(f"[red]Failed: {len(results['failed'])}[/red]")
        console.print(f"[yellow]Skipped: {len(results['skipped'])}[/yellow]")


# ================================
# DEPRECATED FUNCTIONS
# ================================
    # def _start_timing(self):
    #     """Start timing the processing."""
    #     import time
    #     self._start_time = time.time()
    #     if self.verbose:
    #         self.console.print(f"[blue]Processing started at {time.strftime('%H:%M:%S')}[/blue]")
    
    # def _end_timing(self):
    #     """End timing the processing."""
    #     import time
    #     self.end_time = time.time()
    #     if self.verbose:
    #         self.console.print(f"[blue]Processing completed at {time.strftime('%H:%M:%S')}[/blue]")
    
    # def get_processing_time(self) -> Optional[float]:
    #     """Get the total processing time in seconds."""
    #     import time
    #     if self._start_time is None:
    #         return None
    #     end_time = self.end_time if self.end_time is not None else time.time()
    #     return end_time - self._start_time
    
    # def format_processing_time(self) -> str:
    #     """Format processing time in a human-readable format."""
    #     processing_time = self.get_processing_time()
    #     if processing_time is None:
    #         return "Timing not available"
        
    #     hours = int(processing_time // 3600)
    #     minutes = int((processing_time % 3600) // 60)
    #     seconds = int(processing_time % 60)
        
    #     if hours > 0:
    #         return f"{hours}h {minutes}m {seconds}s"
    #     elif minutes > 0:
    #         return f"{minutes}m {seconds}s"
    #     else:
    #         return f"{seconds}s"
    
    
    # def cleanup_problematic_files(directory: Path, verbose: bool = True) -> int:
#     """
#     Clean up problematic files (_top_level and _chunk_) in a directory.
    
#     Args:
#         directory (Path): Directory to clean up
#         verbose (bool): Whether to print verbose output
        
#     Returns:
#         int: Number of files cleaned up
#     """
#     cleaned_count = 0
    
#     if verbose:
#         console = Console()
#         console.print(f"[blue]Cleaning up problematic files in: {directory}[/blue]")
    
#     # Find and remove _top_level files
#     for file_path in directory.rglob("*_top_level.nc"):
#         try:
#             file_path.unlink()
#             cleaned_count += 1
#             if verbose:
#                 console.print(f"[yellow]Removed: {file_path.name}[/yellow]")
#         except Exception as e:
#             if verbose:
#                 console.print(f"[red]Could not remove {file_path.name}: {e}[/red]")
    
#     # Find and remove _chunk_ files
#     for file_path in directory.rglob("*_chunk_*.nc"):
#         try:
#             file_path.unlink()
#             cleaned_count += 1
#             if verbose:
#                 console.print(f"[yellow]Removed: {file_path.name}[/yellow]")
#         except Exception as e:
#             if verbose:
#                 console.print(f"[red]Could not remove {file_path.name}: {e}[/red]")
    
#     if verbose:
#         console.print(f"[green]Cleaned up {cleaned_count} problematic files[/green]")
    
#     return cleaned_count