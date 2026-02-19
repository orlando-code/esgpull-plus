# esgpull-plus

[![Rye](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/astral-sh/rye/main/artwork/badge.json)](https://rye.astral.sh)

API and processing extension to [esgf-download](https://github.com/ESGF/esgf-download): YAML-based download config, fast downloads, [CDO](https://pypi.org/project/cdo/) regridding, and surface/seafloor subsetting.

---

## Contents

1. [Installation and set-up](#installation-and-set-up)
2. [File structure](#file-structure)
3. [Dependencies](#dependencies)
4. [Keeping up with upstream](#keeping-up-with-upstream)
5. [Git configuration](#git-configuration)
6. [Searching for data](#searching-for-data)
7. [CDO regridding pipeline](#cdo-regridding-pipeline)
8. [Works in progress](#works-in-progress)
8. [License](#license)

---

## Installation and set-up

**1. Install the package** (in a conda env if you need CDO regridding):

```bash
pip install esgpull-plus
```

**2. Optional – CDO regridding** (conda recommended):

```bash
conda install -c conda-forge python-cdo
```

**3. Base esgpull:**

```bash
esgpull self install
```

See [esgf-download installation](https://esgf.github.io/esgf-download/installation/).

---

## File structure

```
esgf-download/
├── esgpull/              # Original esgpull
│   └── esgpullplus/      # Extensions (regrid, API, etc.)
├── update-from-upstream.sh
```

---

## Dependencies

- **Base:** from `pyproject.toml` (httpx, click, rich, sqlalchemy, pydantic, etc.).
- **esgpullplus:** pandas, numpy, requests, watchdog, xarray; geospatial via xesmf and `python-cdo` (conda).

---

## Keeping up with upstream

**Recommended:**

```bash
./update-from-upstream.sh
```

**Manual:**

```bash
git fetch upstream && git merge upstream/main
# Then reinstall (conda-aware): conda install -c conda-forge pandas xarray numpy; pip install xesmf cdo-python watchdog orjson
```

---

## Git configuration

```bash
git remote -v
# origin    https://github.com/orlando-code/esgpull-plus/ (fetch/push)
# upstream  https://github.com/ESGF/esgf-download.git (fetch/push)
```

If upstream is missing: `git remote add upstream https://github.com/ESGF/esgf-download.git`

---
## Searching for data

### Main search

Populate the `search.yaml` file (in the repo root) with your ESGF [facets](https://esgf.github.io/esg-search/ESGF_Search_RESTful_API.html) and meta options:

```yaml
search_criteria:
  project: CMIP6
  table_id: Omon
  experiment_id: historical,ssp585
  variable: uo,vo
  filter:
    top_n: 3        # top N datasets to keep
    limit: 10       # max results per sub-search

meta_criteria:
  data_dir: /path/to/data
  max_workers: 4
```

Run the search + download pipeline (uses `search.yaml` automatically):

```bash
python -m esgpull.esgpullplus.api
python -m esgpull.esgpullplus.api --symmetrical  # only download sources with both historical + SSP experiments
```

- **Symmetry:** in `--symmetrical` mode the tool first analyses all experiments and then only downloads datasets from sources that have both historical and SSP-style experiments (e.g. `ssp*`), so historical/SSP are matched.
- **Sorting by resolution:** search results are converted to a DataFrame and sorted by parsed nominal horizontal resolution, then by `dataset_id`, so you always get a consistent “highest resolution first” ordering.
- **Stable IDs:** multi-value facets like `variable: uo,vo` are normalised (split, trimmed, sorted) so the order you write them in `search.yaml` does not affect the generated search IDs or caching.

**Inputs (YAML keys):**

| Key | Description |
|-----|-------------|
| `search_criteria.*` | ESGF facets (project, table_id, experiment_id, variable/variable_id, frequency, etc.). |
| `search_criteria.filter.top_n` | Number of top grouped datasets to keep. |
| `search_criteria.filter.limit` | Maximum number of results per sub-search (useful for debugging). |
| `meta_criteria.data_dir` | Base directory for downloaded data and cached search results. |
| `meta_criteria.max_workers` | Worker count used for any post-download regridding. |

### Search analysis script

`run_search_analysis` runs an ESGF search from `search.yaml`, analyzes source availability (which sources have both historical and SSP experiments, resolutions, ensemble counts), and optionally writes an `analysis_df.csv` plus PNG plots. It ignores `filter.top_n` and `filter.limit` so the analysis uses all matching results.

**Run:**

```bash
python run_search_analysis.py [OPTIONS]
```

| Option | Default | Description |
|--------|--------|-------------|
| `--config` / `--config-path` | `search.yaml` | Path to search config YAML. |
| `--output-dir` | `plots/` (repo) | Directory for `analysis_df.csv` and plot PNGs. |
| `--save-plots` | True | Save plot images (source availability heatmap, ensemble counts, resolution distribution, summary table). |
| `--show-plots` | True | Display plots interactively; pass `--show-plots` to disable. |
| `--require-both` | True | Only include sources that have both historical and SSP experiments. |

**Outputs:** `analysis_df.csv` plus, when `--save-plots` is on, `source_availability_heatmap.png`, `ensemble_counts.png`, `resolution_distribution.png`, `source_summary_table.png` in the output directory. Requires `matplotlib` and `seaborn` for plotting.

---
## CDO regridding pipeline

Single pipeline in `esgpull.esgpullplus.cdo_regrid`: regridding with regrid weights reuse, chunked and parallel processing. Supports **surface** (top level) and **seafloor** extraction: each writes a file next to the original (`*_top_level.nc`, `*_seafloor.nc`) and that file is regridded like any other. Or you can regrid the whole thing.

### Command line

```bash
# Directory: surface only
python -m esgpull.esgpullplus.cdo_regrid /path/to/dir -o /path/to/out -r 1.0 1.0 --extract-surface

# Directory: seafloor only
python -m esgpull.esgpullplus.cdo_regrid /path/to/dir -o /path/to/out --extract-seafloor --max-workers 2

# Both surface and seafloor per file
python -m esgpull.esgpullplus.cdo_regrid /path/to/dir --extreme-levels

# Single file
python -m esgpull.esgpullplus.cdo_regrid /path/to/file.nc -o /path/to/out.nc --extract-seafloor
```

**Options:**

| Option | Default | Description |
|--------|---------|-------------|
| `input` (positional) | required | Input file or directory. |
| `-o`, `--output` | same as input dir | Output file or directory; if omitted, writes next to input. |
| `-r`, `--resolution lon lat` | `1.0 1.0` | Target output resolution (lon_res, lat_res). |
| `-p`, `--pattern` | `"*.nc"` | File pattern when `input` is a directory. |
| `--include-subdirectories` | `True` | Include subdirectories when walking a directory. |
| `--extract-surface` | `False` | Extract and regrid only the top level (surface). |
| `--extract-seafloor` | `False` | Extract and regrid only seafloor values. |
| `--extreme-levels` | `False` | Regrid both surface and seafloor for each file. |
| `--no-regrid-cache` | `False` | Disable reuse of CDO weight files. |
| `--no-seafloor-cache` | `False` | Disable reuse of seafloor depth index cache. |
| `-w`, `--max-workers` | `4` | Maximum parallel workers. |
| `--chunk-size-gb` | `2.0` | Maximum time-chunk size in GB. |
| `--max-memory-gb` | `8.0` | Soft cap for memory-aware chunking. |
| `--no-parallel` | `False` | Process files sequentially. |
| `--no-chunking` | `False` | Disable time chunking (process files in one go). |
| `-v`, `--verbose` | `True` | Verbose progress UI. |
| `--verbose-max` | `False` | Extra diagnostics (grid type, size, large file messages). |
| `--quiet` | `False` | Disable verbose output. |
| `--use-ui` | `True` | Use the rich progress UI. |
| `--unlink-unprocessed` | `False` | Remove any files that could not be processed. |
| `--overwrite` | `False` | Overwrite existing output files. |

N.B. if `--output` is not specified, new files will be written to the same directory as the inputs.

### File watcher regridding

Continuously watch a directory for new NetCDF files and regrid them as they arrive, using the same CDO pipeline. This is helpful when downloading files and wanting them to be processed directly:

```bash
python -m esgpull.esgpullplus.file_watcher /path/to/watch \
  -r 1.0 1.0 \
  --extract-surface \
  --use-regrid-cache \
  --process-existing    # also process files that are already present
```

**Options:**

| Option | Default | Description |
|--------|---------|-------------|
| `watch_dir` (positional) | required | Directory to watch for new NetCDF files. |
| `-r`, `--target-resolution lon lat` | `1.0 1.0` | Target output resolution (lon_res, lat_res). |
| `--target-grid` | `"lonlat"` | CDO target grid type. |
| `--weight-cache-dir` | `None` | Directory to store/reuse CDO weight files. |
| `--max-workers` | `4` | Maximum parallel workers. |
| `--batch-size` | `10` | Maximum files to accumulate before triggering a batch regrid. |
| `--batch-timeout` | `30.0` | Maximum seconds to wait before processing a partial batch. |
| `--extract-surface` | `False` | Extract and regrid only the top level (surface). |
| `--extract-seafloor` | `False` | Extract and regrid only seafloor values. |
| `--use-regrid-cache` | `False` | Enable reuse of CDO weight files. |
| `--use-seafloor-cache` | `False` | Enable reuse of seafloor depth index cache. |
| `--file-settle-seconds` | `10.0` | Wait time to ensure files are no longer being written before processing. |
| `--validate-can-open` | `True` | Validate that files can be opened before scheduling regridding. |
| `--overwrite` | `False` | Overwrite existing regridded outputs. |
| `--delete-original` | `False` | Delete original files after successful regridding. |
| `--process-existing` | `True` | Process files already present in `watch_dir` on startup. |

### Python API

```python
from pathlib import Path
from esgpull.esgpullplus.cdo_regrid import regrid_directory, regrid_single_file, CDORegridPipeline

# Directory
results = regrid_directory(
    Path("data/input"),
    output_dir=Path("data/output"),
    target_resolution=(1.0, 1.0),
    extract_surface=True,
    extract_seafloor=False,
    max_workers=4,
)
# results["successful"], results["failed"], results["skipped"]

# Single file
ok = regrid_single_file(
    Path("data/file.nc"),
    output_dir=Path("data/output"),
    target_resolution=(1.0, 1.0),
    extract_seafloor=True,
)
```

### Features

- **Surface/seafloor:** Writes `*_top_level.nc` or `*_seafloor.nc` beside the original, then regrids that file (same CDO path).
- **Weight reuse:** Weights cached per directory (e.g. `cdo_weights/`); shared when grids match.
- **Chunking:** Large files split by time; optional `--chunk-size-gb`, `--max-memory-gb`.
- **Parallel:** Per-file locking; `--max-workers`; `--no-parallel` to disable.
- **Grids:** Structured, curvilinear, unstructured (e.g. `ncells`); multi-level and time series.


---

## Works in Progress

1. There's a fair bit of functionality here! Time to get a proper documentation site in order...
2. Merge as much of this functionality as is welcome/useful into the original `esgpull` 
repository


I am more than happy to take suggestions/contributions from anyone. Just get in touch via email: rt582@cam.ac.uk

---

## License

Same license terms as the esgpull project.
