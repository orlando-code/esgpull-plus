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

Key inputs:

- **`search_criteria`**: ESGF facets (project, table_id, experiment_id, variable/variable_id, frequency, etc.) plus:
  - **`filter.top_n`**: number of top grouped datasets to keep.
  - **`filter.limit`**: maximum number of results returned per sub-search (useful for debugging).
- **`meta_criteria`**:
  - **`data_dir`**: where downloaded data and search results are stored.
  - **`test`**: dry-run / testing behaviours (see code for details).
  - **`regrid`** (bool): if true, run post-download regridding using `cdo_regrid`.
  - **`regrid_resolution`**: pair `[lon_res, lat_res]` used for that post-download regridding.
  - **`max_workers`**: worker count used for any post-download regridding.

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

**Options (summary):** `-o/--output`, `-r/--resolution` (lon lat), `-p/--pattern`, `--extract-surface`, `--extract-seafloor`, `--extreme-levels`, `--no-regrid-cache`, `--no-seafloor-cache`, `-w/--max-workers`, `--chunk-size-gb`, `--no-parallel`, `--overwrite`, `--quiet`.

N.B. if `output` not specified, new file will be written to the same directory.

### File watcher regridding

Continuously watch a directory for new NetCDF files and regrid them as they arrive, using the same CDO pipeline. This is helpful when downloading files and wanting them to be processed directly:

```bash
python -m esgpull.esgpullplus.file_watcher /path/to/watch \
  -r 1.0 1.0 \
  --extract-surface \
  --use-regrid-cache \
  --process-existing    # also process files that are already present
```

Important arguments:

- **`watch_dir`** (positional): directory to watch.
- **`--target-resolution lon lat`**: target grid resolution (default `1.0 1.0`).
- **`--target-grid`**: CDO target grid type (default `lonlat`).
- **`--weight-cache-dir`**: directory for regrid weights (if omitted, uses per-data-dir cache).
- **`--max-workers`**: maximum parallel workers.
- **`--batch-size` / `--batch-timeout`**: how many files to accumulate and how long to wait before triggering a batch regrid.
- **`--extract-surface` / `--extract-seafloor`**: process only top level, only seafloor, or both if used together with other tools.
- **`--use-regrid-cache` / `--use-seafloor-cache`**: enable weight and seafloor-depth caching.
- **`--file-settle-seconds`**: wait time to ensure files are no longer being written before processing.
- **`--validate-can-open`**: check that files can be opened before scheduling regridding.
- **`--overwrite` / `--delete-original` / `--process-existing`**: control whether outputs overwrite, whether originals are removed, and whether pre-existing files are processed on startup.

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
