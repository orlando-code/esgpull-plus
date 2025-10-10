# esgpull-plus - an API and processing extension to the ESGF data management utility

[![Rye](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/astral-sh/rye/main/artwork/badge.json)](https://rye.astral.sh)

This respository, `esgpull-plus`, modifies and extends the functionality of [`esgpull`](https://github.com/ESGF/esgf-download) by adding an API allowing file download via a `yaml` configuration file. This aims to make the download process more streamlined and improve reproducibility.

In addition - and a work in progress - `esgpull-plus` uses `xesmf` and `cdo` to allow immediate regridding of downloaded CMIP files onto the desired projection. This is useful given that many CMIP models - especially those dealing with ocean variables - output data on unstructured grids.

Finally - also a work in progress - `esgpull-plus` allows file subsetting, both for specified levels and custom subsetting to extract variable conditions at the sea floor.

## Installation and set-up

This repository is a fork of the original [ESGF esgf-download](https://github.com/ESGF/esgf-download) with additional `esgpullplus` functionality. The setup is designed to:

1. Track upstream changes from the original repository
2. Maintain additional dependencies for esgpullplus features
3. Provide easy installation and update procedures using conda


### 1. Initial Installation (Conda - Recommended)

In your virtual environment of choice, install the package using `pip`. N.B. a `conda` environment is required for advanced regridding functionality (via `python-cdo`).
```bash
pip install esgpull-plus
```

### 2. Installation of packages necessary for additional regridding functionality 
`cdo` is a powerful geospatial data tool. It's Python interface, `python-cdo`, is best installed via `conda`:

```bash
conda -c conda-forge install python-cdo
```

### 3. Setting up base `esgpull` functionality

Run

```bash
esgpull self install
```
as described in the original documentation [here](https://esgf.github.io/esgf-download/installation/).


## File Structure

```
esgf-download/
├── esgpull/                    # Original esgpull code
│   └── esgpullplus/           # Your additional functionality
│   └── [original esgpull files and directories]
├── update-from-upstream.sh    # YAML-based update script
```

## Dependencies

### Base Dependencies
The base esgpull dependencies are managed through `pyproject.toml` and include:
- Core Python packages (httpx, click, rich, etc.)
- Database tools (sqlalchemy, alembic)
- Configuration management (pydantic, tomlkit)

### Additional Dependencies (esgpullplus)
As well as the original dependencies, the following are installed via the `pyproject.toml` file to process the downloaded .netcdf files:
- General data handling (pandas, numpy)
- Streamlining downloads (requests, watchdog, rich)
- Geospatial manipulation (xesmf, cdo-python (through `conda`))

## Keeping Up with Upstream (original `esgpull` package)

### Automatic Update (Recommended)

```bash
# Update from upstream and reinstall dependencies
./update-from-upstream.sh
```

This script will:
1. Fetch latest changes from upstream
2. Merge them into your current branch
3. Reinstall all dependencies
4. Verify esgpullplus functionality

### Manual Update

```bash
# Fetch upstream changes
git fetch upstream

# Merge into your branch
git merge upstream/main

# Reinstall dependencies (conda-aware)
if command -v conda &> /dev/null; then
    conda install -c conda-forge -y pandas xarray numpy requests
    pip install xesmf cdo-python watchdog orjson
else
    pip install -r requirements-plus.txt
fi
```

## Git Configuration

Your repository should have these remotes configured:

```bash
# Check current remotes
git remote -v

# Should show:
# origin    https://github.com/orlando-code/esgpull-plus/ (fetch)
# origin    https://github.com/orlando-code/esgpull-plus/ (push)
# upstream  https://github.com/ESGF/esgf-download.git (fetch)
# upstream  https://github.com/ESGF/esgf-download.git (push)
```

If upstream is not configured:

```bash
git remote add upstream https://github.com/ESGF/esgf-download.git
```

---
Everything below this is copied directly from the original `esgpull` repository.

```py
from esgpull import Esgpull, Query

query = Query()
query.selection.project = "CMIP6"
query.options.distrib = True  # default=False
esg = Esgpull()
nb_datasets = esg.context.hits(query, file=False)[0]
nb_files = esg.context.hits(query, file=True)[0]
datasets = esg.context.datasets(query, max_hits=5)
print(f"Number of CMIP6 datasets: {nb_datasets}")
print(f"Number of CMIP6 files: {nb_files}")
for dataset in datasets:
    print(dataset)
```

## Features

- Command-line interface
- HTTP download (async multi-file)

## Installation

`esgpull` is distributed via PyPI:

```shell
pip install esgpull
esgpull --help
```

For isolated installation, [`uv`](https://github.com/astral-sh/uv) or
[`pipx`](https://github.com/pypa/pipx) are recommended:

```shell
# with uv
uv tool install esgpull
esgpull --help

# alternatively, uvx enables running without explicit installation (comes with uv)
uvx esgpull --help
```

```shell
# with pipx
pipx install esgpull
esgpull --help
```

## Usage

```console
Usage: esgpull [OPTIONS] COMMAND [ARGS]...

  esgpull is a management utility for files and datasets from ESGF.

Options:
  -V, --version  Show the version and exit.
  -h, --help     Show this message and exit.

Commands:
  add       Add queries to the database
  config    View/modify config
  convert   Convert synda selection files to esgpull queries
  download  Asynchronously download files linked to queries
  login     OpenID authentication and certificates renewal
  remove    Remove queries from the database
  retry     Re-queue failed and cancelled downloads
  search    Search datasets and files on ESGF
  self      Manage esgpull installations / import synda database
  show      View query tree
  status    View file queue status
  track     Track queries
  untrack   Untrack queries
  update    Fetch files, link files <-> queries, send files to download...
```

## Useful links
* [ESGF Webinar: An Introduction to esgpull, A Replacement for Synda](https://www.youtube.com/watch?v=xv2RVMd1iCA)


## Contributions

You can use the common github workflow (through pull requests and issues) to contribute.
