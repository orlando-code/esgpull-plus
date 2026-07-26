"""Search-only CLI (no download)."""

from __future__ import annotations

import sys
from pathlib import Path

from rich.console import Console

from esgpull.esgpullplus import fileops, search


def run_search(config_path: str | Path | None = None) -> list:
    """
    Run ESGF search from YAML and print a summary. Does not download files.

    Returns the list of :class:`~esgpull.esgpullplus.enhanced_file.EnhancedFile`
    records matched after filters (time subset, top_n, limit).
    """
    path = fileops.resolve_search_config_path(config_path)
    console = Console()

    if not path.exists():
        console.print(f"[red bold]Configuration error:[/red bold] Search config not found: {path}")
        sys.exit(1)

    console.print(f"[cyan]Using search config: {path}[/cyan]")
    config_dict = fileops.read_yaml(path)
    if not config_dict:
        console.print(
            f"[red bold]Configuration error:[/red bold] Could not read search config: {path}"
        )
        sys.exit(1)

    search_criteria = config_dict.get("search_criteria", {})
    meta_criteria = config_dict.get("meta_criteria", {})

    try:
        search.validate_limiting_search_criteria(search_criteria)
    except ValueError as exc:
        console.print(f"[red bold]Configuration error:[/red bold] {exc}")
        sys.exit(1)

    start = fileops.print_timestamp(console, "START")
    search_results = search.SearchResults(
        search_criteria=search_criteria,
        meta_criteria=meta_criteria,
        file=True,
    )
    files = search_results.run()
    end = fileops.print_timestamp(console, "END")
    elapsed = fileops.format_processing_time(
        fileops.get_processing_time(start, end)
    )

    n = len(files)
    label = "file" if n == 1 else "files"
    console.print(f"[green]Search complete:[/green] {n} {label} matched ({elapsed})")
    if n and search_results.results_df is not None:
        console.print(
            f"[dim]Results cached under: {search_results.search_results_dir}[/dim]"
        )
    return files
