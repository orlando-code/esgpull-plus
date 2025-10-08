#!/bin/bash

# Installation script for esgpullplus using conda
# This script sets up the conda environment with both upstream and additional dependencies

set -e  # Exit on any error

echo "🚀 Setting up esgpullplus conda environment..."

# Check if conda is available
if ! command -v conda &> /dev/null; then
    echo "❌ Conda not found. Please install Miniconda or Anaconda first:"
    echo "   https://docs.conda.io/en/latest/miniconda.html"
    exit 1
fi

# Determine target conda environment name
# Prefer active env, else use ESGP_ENV, else default to "esgpullplus"
ENV_NAME="${CONDA_DEFAULT_ENV}"
if [[ "$ENV_NAME" == "" ]]; then
    ENV_NAME="${ESGP_ENV:-esgpullplus}"
fi
echo "📌 Using conda environment: $ENV_NAME"

# Determine which environment file to use
if [[ "$1" == "--dev" ]]; then
    ENV_FILE="environment-dev.yml"
    echo "📦 Using development environment file: $ENV_FILE"
elif [[ "$1" == "--plus" ]]; then
    ENV_FILE="environment-plus.yml"
    echo "📦 Using plus environment file: $ENV_FILE"
else
    ENV_FILE="environment.yml"
    echo "📦 Using standard environment file: $ENV_FILE"
fi

# Check if environment file exists
if [[ ! -f "$ENV_FILE" ]]; then
    echo "❌ Environment file $ENV_FILE not found!"
    echo "   Available files:"
    ls -la environment*.yml 2>/dev/null || echo "   No environment*.yml files found"
    exit 1
fi

# Create or update environment from YAML file
echo "📦 Creating/updating conda environment from $ENV_FILE..."
if conda env list | awk '{print $1}' | grep -qx "$ENV_NAME"; then
    echo "   Updating existing environment '$ENV_NAME'..."
    conda env update -n "$ENV_NAME" -f "$ENV_FILE"
else
    echo "   Creating new environment '$ENV_NAME'..."
    # Temporarily modify the YAML to use our environment name
    sed "s/name: .*/name: $ENV_NAME/" "$ENV_FILE" > "/tmp/env_${ENV_NAME}.yml"
    conda env create -f "/tmp/env_${ENV_NAME}.yml"
    rm "/tmp/env_${ENV_NAME}.yml"
fi

# Install esgpull in development mode (this is crucial for the package to be found)
echo "📦 Installing esgpull in development mode..."
conda run -n "$ENV_NAME" python3 -m pip install -e .

# Verify installation
echo "✅ Verifying installation (env: $ENV_NAME)..."
conda run -n "$ENV_NAME" python3 -c "
import esgpull
import esgpull.esgpullplus
print('✅ Base esgpull imported successfully')
print('✅ esgpullplus module imported successfully')

# Test key imports
try:
    import pandas
    import xarray
    import xesmf
    import watchdog
    print('✅ All esgpullplus dependencies available')
except ImportError as e:
    print(f'❌ Missing dependency: {e}')
    exit(1)
"

echo ""
echo "🎉 Installation complete!"
echo ""
echo "To keep up with upstream changes:"
echo "  1. git fetch upstream"
echo "  2. git merge upstream/main  # or upstream/develop"
echo "  3. ./update-from-upstream.sh  # Automated update script"
echo ""
echo "To run esgpullplus:"
echo "  esgpull --help  # Base functionality"
echo "  # Your custom esgpullplus commands will be available"
echo ""
echo "Environment info:"
echo "  Conda environment: $ENV_NAME"
echo "  Python version: $(conda run -n "$ENV_NAME" python3 --version)"
