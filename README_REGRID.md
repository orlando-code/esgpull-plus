# CDO Regridding Pipeline

A robust, efficient CDO-based regridding pipeline for large NetCDF files with intelligent weight reuse and memory optimization.

## Features

### Core Capabilities
- **CDO-only processing**: No xESMF dependency, uses only CDO for regridding
- **Weight reuse**: Intelligent caching and reuse of regrid weights for files with identical grids
- **Memory optimization**: Chunked processing for large files (>GB)
- **Top-level selection**: Automatic selection of top level for multi-level files
- **Parallel processing**: Multi-threaded batch processing
- **Comprehensive error handling**: Detailed logging and error recovery

### Advanced Features
- **Grid signature detection**: Automatic detection of grid types and reuse of weights
- **Chunked processing**: Large files are automatically split into time chunks
- **Memory monitoring**: Real-time memory usage tracking
- **Batch processing**: Efficient processing of files in the same directory
- **Weight management**: Automatic cleanup of old weight files

## Installation

The pipeline requires the following dependencies:

```bash
pip install xarray cdo netcdf4 numpy psutil rich
```

Make sure CDO is installed on your system:
```bash
# Ubuntu/Debian
sudo apt-get install cdo

# CentOS/RHEL
sudo yum install cdo

# macOS
brew install cdo
```

## Quick Start

### Basic Usage

```python
from pathlib import Path
from esgpull.esgpullplus.cdo_regrid_pipeline import regrid_directory

# Regrid all files in a directory
results = regrid_directory(
    input_dir=Path("data/input"),
    output_dir=Path("data/output"),
    target_resolution=(1.0, 1.0),  # 1 degree resolution
    file_pattern="*.nc",
    top_level_only=True,
    verbose=True,
)

print(f"Successful: {len(results['successful'])}")
print(f"Failed: {len(results['failed'])}")
print(f"Skipped: {len(results['skipped'])}")
```

### Advanced Usage

```python
from esgpull.esgpullplus.advanced_cdo_regrid import AdvancedCDORegridPipeline

# Create advanced pipeline
pipeline = AdvancedCDORegridPipeline(
    target_resolution=(0.5, 0.5),  # 0.5 degree resolution
    top_level_only=True,
    verbose=True,
    max_workers=4,  # Use 4 parallel workers
    chunk_size_gb=2.0,  # Chunk files larger than 2GB
    enable_parallel=True,
)

# Regrid single file
success = pipeline.regrid_file_advanced(
    input_path=Path("data/large_file.nc"),
    output_path=Path("data/large_file_regridded.nc")
)

# Print statistics
pipeline.print_statistics()
```

## Command Line Usage

### Basic Pipeline

```bash
python -m esgpull.esgpullplus.cdo_regrid_pipeline \
    /path/to/input/directory \
    --output-dir /path/to/output/directory \
    --resolution 1.0 1.0 \
    --pattern "*.nc" \
    --top-level-only \
    --verbose
```

### Advanced Pipeline

```bash
python -m esgpull.esgpullplus.advanced_cdo_regrid \
    /path/to/input/directory \
    --output-dir /path/to/output/directory \
    --resolution 0.5 0.5 \
    --pattern "*.nc" \
    --top-level-only \
    --max-workers 4 \
    --chunk-size-gb 2.0 \
    --verbose
```

## Configuration Options

### Basic Pipeline Options

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `target_resolution` | tuple | (1.0, 1.0) | Target resolution as (lon_res, lat_res) |
| `target_grid` | str | "lonlat" | Target grid type |
| `weight_cache_dir` | Path | None | Directory for weight cache |
| `top_level_only` | bool | True | Only process top level for multi-level files |
| `verbose` | bool | True | Enable verbose output |

### Advanced Pipeline Options

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `max_memory_gb` | float | 8.0 | Maximum memory usage in GB |
| `max_workers` | int | None | Maximum parallel workers |
| `chunk_size_gb` | float | 2.0 | Maximum chunk size in GB |
| `enable_parallel` | bool | True | Enable parallel processing |

## File Types and Grid Support

### Supported File Types
- **Structured grids**: Regular lon/lat grids with 1D coordinates
- **Curvilinear grids**: Grids with (j,i) dimensions and 2D lat/lon coordinates
- **Unstructured grids**: Files with `ncells` dimension
- **Multi-level files**: Files with depth/level dimensions
- **Time series**: Files with time dimension

### Grid Detection
The pipeline automatically detects:
- **Structured grids**: 1D lat/lon coordinates
- **Curvilinear grids**: (j,i) dimensions with 2D lat/lon coordinates
- **Unstructured grids**: `ncells` dimension
- **Coordinate names**: (lon/longitude, lat/latitude)
- **Dimension structure**: All grid dimensions
- **Level information**: Depth/level dimensions

### Weight Reuse Strategy
- Files in the same directory are assumed to have the same grid
- Grid signatures are computed and cached
- Weights are reused for files with identical grid signatures
- Old weights are automatically cleaned up

## Memory Management

### Chunked Processing
Large files are automatically processed in chunks when:
- File size > `chunk_size_gb`
- Estimated memory usage > `max_memory_gb`
- Time steps > 100

### Memory Monitoring
- Real-time memory usage tracking
- Peak memory usage reporting
- Automatic chunking for memory-constrained systems

## Performance Optimization

### CDO Configuration
The pipeline automatically configures CDO for optimal performance:
- Disables compression for speed
- Enables parallel I/O
- Uses 64-bit offsets for large files
- Configures optimal thread count

### Parallel Processing
- Multi-threaded batch processing
- Process-based parallelization for CPU-intensive tasks
- Automatic worker count optimization

## Error Handling

### Comprehensive Error Reporting
- Detailed error messages with context
- Full traceback information
- File-specific error logging
- Recovery suggestions

### Common Error Types
- **Memory errors**: Automatic chunking and memory optimization
- **Grid errors**: Automatic grid detection and validation
- **File errors**: Comprehensive file validation
- **CDO errors**: Detailed CDO error reporting

## Examples

### Example 1: Basic Regridding

```python
from esgpull.esgpullplus.cdo_regrid_pipeline import CDORegridPipeline

# Create pipeline
pipeline = CDORegridPipeline(
    target_resolution=(1.0, 1.0),
    top_level_only=True,
    verbose=True,
)

# Regrid single file
success = pipeline.regrid_file(
    input_path=Path("data/input.nc"),
    output_path=Path("data/output.nc")
)
```

### Example 2: Batch Processing

```python
from esgpull.esgpullplus.advanced_cdo_regrid import regrid_directory_advanced

# Process entire directory
results = regrid_directory_advanced(
    input_dir=Path("data/input"),
    output_dir=Path("data/output"),
    target_resolution=(0.5, 0.5),
    file_pattern="*.nc",
    top_level_only=True,
    verbose=True,
    max_workers=4,
    chunk_size_gb=2.0,
    enable_parallel=True,
)
```

### Example 3: Enhanced Grid Support

```python
from esgpull.esgpullplus.enhanced_cdo_regrid import EnhancedCDORegridPipeline

# Create enhanced pipeline with comprehensive grid support
pipeline = EnhancedCDORegridPipeline(
    target_resolution=(0.5, 0.5),
    top_level_only=True,
    verbose=True,
)

# Process file with automatic grid detection
success = pipeline.regrid_file(
    input_path=Path("data/curvilinear_file.nc"),
    output_path=Path("data/curvilinear_file_regridded.nc")
)

# Print statistics including grid type breakdown
pipeline.print_statistics()
```

### Example 4: Memory-Constrained Processing

```python
from esgpull.esgpullplus.advanced_cdo_regrid import AdvancedCDORegridPipeline

# Create memory-constrained pipeline
pipeline = AdvancedCDORegridPipeline(
    target_resolution=(0.25, 0.25),
    top_level_only=True,
    verbose=True,
    max_memory_gb=4.0,  # Limit to 4GB
    chunk_size_gb=1.0,  # More aggressive chunking
    enable_parallel=True,
)

# Process large file
success = pipeline.regrid_file_advanced(
    input_path=Path("data/very_large_file.nc"),
    output_path=Path("data/very_large_file_regridded.nc")
)
```

## Troubleshooting

### Common Issues

1. **CDO not found**: Make sure CDO is installed and in PATH
2. **Memory errors**: Reduce `chunk_size_gb` or `max_memory_gb`
3. **Grid errors**: Check file format and coordinate names
4. **Permission errors**: Ensure write access to output directory

### Debug Mode

Enable verbose output for detailed debugging:

```python
pipeline = CDORegridPipeline(verbose=True)
```

### Logging

The pipeline provides comprehensive logging:

```python
import logging
logging.basicConfig(level=logging.INFO)
```

## Performance Tips

1. **Use weight caching**: Keep weights in a persistent directory
2. **Batch similar files**: Group files with identical grids
3. **Optimize memory**: Adjust `chunk_size_gb` for your system
4. **Use parallel processing**: Enable for multiple files
5. **Select top level**: Use `top_level_only=True` for multi-level files

## License

This module is part of the esgpull project and follows the same license terms.
