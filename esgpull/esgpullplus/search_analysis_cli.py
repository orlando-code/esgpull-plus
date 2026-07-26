"""Search + source-availability analysis CLI."""

from __future__ import annotations

import sys
import traceback
from pathlib import Path

from rich.console import Console

from esgpull.esgpullplus import config, fileops
from esgpull.esgpullplus.search import SearchResults


def run_search_analysis(
    config_path: str | Path | None = None,
    output_dir: str | Path | None = None,
    *,
    show_plots: bool = True,
    save_plots: bool = True,
    require_both: bool = True,
) -> None:
    """
    Run ESGF search from YAML, analyse source availability, and optionally save plots.

    Ignores ``filter.top_n`` and ``filter.limit`` so analysis uses all matching results.
    """
    path = fileops.resolve_search_config_path(config_path)
    console = Console()

    if not path.exists():
        console.print(f"[red bold]Error:[/red bold] Configuration file not found: {path}")
        sys.exit(1)

    console.print(f"[cyan]Reading configuration from {path}[/cyan]")
    config_dict = fileops.read_yaml(path)
    search_criteria = config_dict.get("search_criteria", {})
    meta_criteria = config_dict.get("meta_criteria", {})

    if not search_criteria:
        console.print("[red bold]Error:[/red bold] No search_criteria found in configuration file")
        sys.exit(1)

    search_criteria = dict(search_criteria)
    filt = dict(search_criteria.get("filter") or {})
    filt["top_n"] = None
    filt["limit"] = None
    search_criteria["filter"] = filt

    start_time = fileops.print_timestamp(console, "Starting search and analysis")
    search_results = SearchResults(
        search_criteria=search_criteria,
        meta_criteria=meta_criteria,
    )

    console.print("\nPerforming search (will use cache if available)...")
    try:
        search_results.run()
    except Exception as exc:
        console.print(f"[red]Error during search: {exc}[/red]")
        sys.exit(1)

    config.plots_dir.mkdir(parents=True, exist_ok=True)
    out = Path(output_dir) if output_dir else config.plots_dir
    out.mkdir(parents=True, exist_ok=True)

    console.print(f"\nAnalyzing source availability and saving to {out}...")
    try:
        analysis_df, plots = search_results.analyze_and_visualize_sources(
            output_dir=str(out),
            show_plots=show_plots,
            require_both=require_both,
            save_plots=save_plots,
        )
        console.print("\n[green]Analysis complete![/green]")
        console.print(f"Found {len(analysis_df)} sources matching criteria")
        console.print(f"Analysis DataFrame saved to: {out / 'analysis_df.csv'}")
        if plots:
            console.print(f"Plots saved to: {out}")
            for plot_name, plot_path in plots.items():
                console.print(f"  - {plot_name}: {plot_path}")
    except Exception as exc:
        console.print(f"[red]Error during analysis: {exc}[/red]")
        traceback.print_exc()
        sys.exit(1)

    end_time = fileops.print_timestamp(console, "Ending search and analysis")
    processing_time = fileops.get_processing_time(start_time, end_time)
    console.print(
        f"[dim]Processing time: {fileops.format_processing_time(processing_time)}[/dim]"
    )
