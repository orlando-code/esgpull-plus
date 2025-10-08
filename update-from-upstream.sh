#!/bin/bash

# Script to update from upstream while preserving esgpullplus functionality
# This handles the workflow of pulling upstream changes and reinstalling dependencies

set -e  # Exit on any error

echo "🔄 Updating from upstream..."

# Check if we're in a git repository
if ! git rev-parse --git-dir > /dev/null 2>&1; then
    echo "❌ Not in a git repository"
    exit 1
fi

# Check if upstream remote exists
if ! git remote | grep -q upstream; then
    echo "❌ No upstream remote found. Please add it with:"
    echo "   git remote add upstream https://github.com/ESGF/esgf-download.git"
    exit 1
fi

# Stash any uncommitted changes
if ! git diff --quiet || ! git diff --cached --quiet; then
    echo "📦 Stashing uncommitted changes..."
    git stash push -m "Auto-stash before upstream update $(date)"
    STASHED=true
else
    STASHED=false
fi

# Fetch latest from upstream
echo "📥 Fetching latest from upstream..."
git fetch upstream

# Get current branch
CURRENT_BRANCH=$(git branch --show-current)
echo "📍 Current branch: $CURRENT_BRANCH"

# Merge upstream changes
echo "🔄 Merging upstream changes..."
if git merge upstream/main; then
    echo "✅ Successfully merged upstream changes"
else
    echo "⚠️  Merge conflicts detected. Please resolve them manually."
    echo "   After resolving conflicts, run:"
    echo "   git add ."
    echo "   git commit"
    echo "   ./update-from-upstream.sh --continue"
    
    if [[ "$1" != "--continue" ]]; then
        exit 1
    fi
fi

# Reinstall dependencies to ensure compatibility
echo "📦 Reinstalling dependencies..."

# Determine target conda environment name (same logic as install script)
ENV_NAME="${CONDA_DEFAULT_ENV}"
if [[ "$ENV_NAME" == "" ]]; then
    ENV_NAME="${ESGP_ENV:-esgpullplus}"
fi
echo "📌 Using conda environment: $ENV_NAME"

if command -v conda &> /dev/null; then
    # Determine which environment file to use (prefer standard, fallback to dev)
    ENV_FILE="environment-plus.yml"
    if [[ ! -f "$ENV_FILE" ]]; then
        ENV_FILE="environment-dev.yml"
    fi
    
    if [[ -f "$ENV_FILE" ]]; then
        echo "   Updating conda environment from $ENV_FILE..."
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
        echo "   Installing esgpull in development mode..."
        conda run -n "$ENV_NAME" python3 -m pip install -e .
    else
        echo "   No environment YAML file found, falling back to pip installation..."
        # Fallback to pip installation
        if conda env list | awk '{print $1}' | grep -qx "$ENV_NAME"; then
            conda run -n "$ENV_NAME" python3 -m pip install -e .
            if [[ -f "requirements-plus.txt" ]]; then
                conda run -n "$ENV_NAME" python3 -m pip install -r requirements-plus.txt
            fi
        else
            echo "   Creating basic conda environment..."
            conda create -y -n "$ENV_NAME" -c conda-forge python=3.11 pip
            conda run -n "$ENV_NAME" python3 -m pip install -e .
            if [[ -f "requirements-plus.txt" ]]; then
                conda run -n "$ENV_NAME" python3 -m pip install -r requirements-plus.txt
            fi
        fi
    fi
else
    echo "   Conda not found, using pip only..."
    pip install -e .
    pip install -r requirements-plus.txt
fi

# Restore stashed changes if any
if [[ "$STASHED" == "true" ]]; then
    echo "📦 Restoring stashed changes..."
    if git stash pop; then
        echo "✅ Successfully restored stashed changes"
    else
        echo "⚠️  Stash pop had conflicts. Please resolve manually:"
        echo "   git status"
        echo "   # Resolve conflicts, then:"
        echo "   git add ."
        echo "   git stash drop"
    fi
fi

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
except ImportError:
    missing.append("watchdog")

if missing:
    print(f"❌ Missing dependencies: {', '.join(missing)}")
    exit(1)
else:
    print('✅ All esgpullplus dependencies available')
"

echo ""
echo "🎉 Update complete!"
echo ""
echo "Summary:"
echo "  - Fetched latest from upstream"
echo "  - Merged changes into $CURRENT_BRANCH"
echo "  - Reinstalled all dependencies"
echo "  - Verified esgpullplus functionality"
echo ""
echo "Your esgpullplus functionality should be preserved and up-to-date."
