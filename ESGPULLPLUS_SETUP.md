# ESGPullPlus Setup Guide

This document explains how to set up and maintain the esgpullplus environment, which extends the base ESGF esgpull tool with additional functionality.

## Overview

This repository is a fork of the original [ESGF esgf-download](https://github.com/ESGF/esgf-download) with additional `esgpullplus` functionality. The setup is designed to:

1. Track upstream changes from the original repository
2. Maintain additional dependencies for esgpullplus features
3. Provide easy installation and update procedures using conda

## Quick Start

### 1. Initial Setup (Conda - Recommended)

```bash
# Clone your fork (if not already done)
git clone <your-fork-url>
cd esgf-download

# Option A: Create conda environment from environment.yml (easiest)
conda env create -f environment.yml
conda activate esgpullplus

# Option B: Create conda environment manually
conda create -n esgpullplus python=3.11
conda activate esgpullplus
./install-plus.sh
```

### 2. Development Setup (Optional)

```bash
# Install with development dependencies
./install-plus.sh --dev
```

### 3. Alternative: Virtual Environment (if conda not available)

```bash
# Create and activate virtual environment
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install everything (will use pip only)
./install-plus.sh
```

## File Structure

```
esgf-download/
├── esgpull/                    # Original esgpull code
│   └── esgpullplus/           # Your additional functionality
├── environment-plus.yml        # Main conda environment specification
├── environment-dev.yml         # Development conda environment specification
├── environment.yml             # Legacy conda environment specification
├── install-plus.sh            # Installation script (YAML-based)
├── update-from-upstream.sh    # Update script (YAML-based)
├── Makefile                   # Convenient commands
└── ESGPULLPLUS_SETUP.md      # This file
```

## Dependencies

### Base Dependencies
The base esgpull dependencies are managed through `pyproject.toml` and include:
- Core Python packages (httpx, click, rich, etc.)
- Database tools (sqlalchemy, alembic)
- Configuration management (pydantic, tomlkit)

### Additional Dependencies (esgpullplus)
Dependencies are now managed through YAML files for native conda support:

**`environment-plus.yml` - Main environment specification:**
- **Conda packages**: pandas, xarray, numpy, requests (scientific computing)
- **Pip packages**: xesmf, cdo-python, watchdog, orjson, rich (specialized)
- **Development mode**: Installs esgpull in editable mode

**`environment-dev.yml` - Development environment specification:**
- **All packages from environment-plus.yml**
- **Additional dev tools**: pytest, black, isort, flake8, mypy, jupyter
- **Type stubs**: types-requests, types-python-dateutil

**Legacy files (fallback support):**
- `requirements-conda.txt`, `requirements-pip.txt`, `requirements-plus.txt`

## Keeping Up with Upstream

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
# origin    <your-fork-url> (fetch)
# origin    <your-fork-url> (push)
# upstream  https://github.com/ESGF/esgf-download.git (fetch)
# upstream  https://github.com/ESGF/esgf-download.git (push)
```

If upstream is not configured:

```bash
git remote add upstream https://github.com/ESGF/esgf-download.git
```

## Development Workflow

### Adding New Dependencies

1. **For conda packages** (scientific computing, better dependency resolution):
   Edit `environment-plus.yml` and add to the `dependencies` section:
   ```yaml
   dependencies:
     - new-package>=1.0.0
   ```

2. **For pip packages** (specialized packages):
   Edit `environment-plus.yml` and add to the `pip` section:
   ```yaml
   - pip:
     - new-package>=1.0.0
   ```

3. **For development dependencies**:
   Edit `environment-dev.yml` similarly

4. Install and test:
   ```bash
   ./install-plus.sh
   ```

5. Commit the changes:
   ```bash
   git add environment*.yml
   git commit -m "Add new-package dependency"
   ```

### Alternative: Direct Conda Commands

You can also use conda directly:
```bash
# Create from YAML
conda env create -f environment-plus.yml

# Update existing environment
conda env update -n esgpullplus -f environment-plus.yml

# Export current environment
conda env export > environment-current.yml
```

### Testing After Updates

After updating from upstream, always test your esgpullplus functionality:

```bash
# Test imports
python -c "import esgpull.esgpullplus; print('✅ esgpullplus imports successfully')"

# Test your specific functionality
# (Run your custom commands/tests here)
```

## Troubleshooting

### Import Errors After Update

If you get import errors after updating:

```bash
# Reinstall all dependencies
pip install -e .
pip install -r requirements-plus.txt

# Or use the update script
./update-from-upstream.sh
```

### Merge Conflicts

If you encounter merge conflicts during updates:

1. Resolve conflicts manually
2. Test your changes
3. Commit the resolution
4. Continue with dependency reinstallation

### Conda Environment Issues

If you have issues with your conda environment:

```bash
# Remove and recreate conda environment
conda deactivate
conda env remove -n esgpullplus
conda env create -f environment.yml
conda activate esgpullplus
```

### Virtual Environment Issues (fallback)

If you have issues with your virtual environment:

```bash
# Deactivate and recreate
deactivate
rm -rf venv
python -m venv venv
source venv/bin/activate
./install-plus.sh
```

## Best Practices

1. **Use conda environments** for better dependency management (especially for scientific packages)
2. **Test after every upstream update** to ensure compatibility
3. **Keep your esgpullplus code in the `esgpull/esgpullplus/` directory**
4. **Document any new dependencies** in both `environment.yml` and `requirements-plus.txt`
5. **Use the provided scripts** for installation and updates
6. **Commit your esgpullplus changes** before updating from upstream
7. **Use conda for scientific packages** (pandas, xarray, numpy) and pip for specialized ones

## Contributing Back

If you develop useful features that could benefit the upstream project:

1. Test thoroughly
2. Document your changes
3. Consider submitting a pull request to the upstream repository
4. Maintain your fork with the additional esgpullplus functionality

## Support

For issues with:
- **Base esgpull functionality**: Check the [original repository](https://github.com/ESGF/esgf-download)
- **esgpullplus functionality**: Check your implementation and dependencies
- **Setup issues**: Review this guide and the installation scripts
