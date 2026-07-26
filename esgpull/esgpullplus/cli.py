#!/usr/bin/env python3
"""Unified ``esgplus`` command-line interface."""

from __future__ import annotations

import click

from esgpull.esgpullplus import fileops
from esgpull.esgpullplus.api import run_download
from esgpull.esgpullplus.search_analysis_cli import run_search_analysis
from esgpull.esgpullplus.search_cli import run_search


def _default_config() -> str:
    return str(fileops.resolve_search_config_path())


@click.group()
@click.version_option(package_name="esgpull-plus")
def main() -> None:
    """ESGF search, download, and analysis for esgpull-plus."""


@main.command("download")
@click.option(
    "--config",
    "--config-path",
    "config_path",
    default=_default_config,
    show_default=True,
    help="Path to search YAML configuration file.",
)
@click.option(
    "--symmetrical",
    is_flag=True,
    help="Only download sources with both historical and SSP experiments.",
)
def download_cmd(config_path: str, symmetrical: bool) -> None:
    """Search ESGF and download matching files."""
    run_download(symmetrical=symmetrical, config_path=config_path)


@main.command("search")
@click.option(
    "--config",
    "--config-path",
    "config_path",
    default=_default_config,
    show_default=True,
    help="Path to search YAML configuration file.",
)
def search_cmd(config_path: str) -> None:
    """Run ESGF search only (no download)."""
    run_search(config_path=config_path)


@main.command("search-analysis")
@click.option(
    "--config",
    "--config-path",
    "config_path",
    default=_default_config,
    show_default=True,
    help="Path to search YAML configuration file.",
)
@click.option(
    "--output-dir",
    default=None,
    help="Directory for analysis_df.csv and plots (default: plots/ in repo).",
)
@click.option("--save-plots/--no-save-plots", default=True, help="Save plot PNGs.")
@click.option("--show-plots/--no-show-plots", default=True, help="Display plots interactively.")
@click.option(
    "--require-both/--any-experiment",
    default=True,
    help="Only include sources with both historical and SSP experiments.",
)
def search_analysis_cmd(
    config_path: str,
    output_dir: str | None,
    save_plots: bool,
    show_plots: bool,
    require_both: bool,
) -> None:
    """Search ESGF and analyse source availability (CSV + optional plots)."""
    run_search_analysis(
        config_path=config_path,
        output_dir=output_dir,
        show_plots=show_plots,
        save_plots=save_plots,
        require_both=require_both,
    )


if __name__ == "__main__":
    main()
