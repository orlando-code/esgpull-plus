# Makefile for esgpullplus development

.PHONY: help install install-dev install-conda update test clean

help: ## Show this help message
	@echo "ESGPullPlus Development Commands:"
	@echo ""
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-20s\033[0m %s\n", $$1, $$2}'

install: ## Install base and esgpullplus dependencies (conda-based)
	@echo "Installing esgpullplus environment..."
	@./install-plus.sh

install-dev: ## Install with development dependencies (conda-based)
	@echo "Installing esgpullplus with dev dependencies..."
	@./install-plus.sh --dev

install-conda: ## Create conda environment from environment-plus.yml
	@echo "Creating conda environment from environment-plus.yml..."
	@conda env create -f environment-plus.yml
	@echo "Environment created! Activate with: conda activate esgpullplus"

install-conda-dev: ## Create conda environment from environment-dev.yml
	@echo "Creating conda environment from environment-dev.yml..."
	@conda env create -f environment-dev.yml
	@echo "Environment created! Activate with: conda activate esgpullplus-dev"

update-conda: ## Update conda environment from environment-plus.yml
	@echo "Updating conda environment from environment-plus.yml..."
	@conda env update -n esgpullplus -f environment-plus.yml

update: ## Update from upstream and reinstall dependencies
	@echo "Updating from upstream..."
	@./update-from-upstream.sh

test: ## Run basic import tests
	@echo "Testing esgpullplus imports..."
	@python -c "import esgpull; import esgpull.esgpullplus; print('✅ All imports successful')"

test-deps: ## Test all esgpullplus dependencies
	@echo "Testing esgpullplus dependencies..."
	@python -c "import pandas, xarray, xesmf, watchdog, numpy, requests; print('✅ All dependencies available')"

clean: ## Clean up temporary files
	@echo "Cleaning up..."
	@find . -type f -name "*.pyc" -delete
	@find . -type d -name "__pycache__" -delete
	@find . -type d -name "*.egg-info" -exec rm -rf {} +

setup-git: ## Setup git remotes (run once)
	@echo "Setting up git remotes..."
	@git remote add upstream https://github.com/ESGF/esgf-download.git || echo "Upstream remote already exists"
	@echo "Git remotes configured:"
	@git remote -v

status: ## Show current git and environment status
	@echo "=== Git Status ==="
	@git status --short
	@echo ""
	@echo "=== Git Remotes ==="
	@git remote -v
	@echo ""
	@echo "=== Python Environment ==="
	@python --version
	@echo "Conda env: $$CONDA_DEFAULT_ENV"
	@echo "Virtual env: $$VIRTUAL_ENV"
	@echo ""
	@echo "=== Installed Packages ==="
	@pip list | grep -E "(esgpull|pandas|xarray|xesmf|watchdog)" || echo "No relevant packages found"
