# esgpull-plus

[![Rye](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/astral-sh/rye/main/artwork/badge.json)](https://rye.astral.sh)

API and processing extension to [esgf-download](https://github.com/ESGF/esgf-download): YAML-based download config, fast downloads, CDO regridding, and surface/seafloor subsetting.

---

## Contents

1. [Installation and set-up](#installation-and-set-up)
2. [File structure](#file-structure)
3. [Dependencies](#dependencies)
4. [Keeping up with upstream](#keeping-up-with-upstream)
5. [Git configuration](#git-configuration)
6. [CDO regridding pipeline](#cdo-regridding-pipeline)
7. [License](#license)

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

## License

Same license terms as the esgpull project.
