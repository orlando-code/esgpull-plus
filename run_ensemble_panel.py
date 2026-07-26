#!/usr/bin/env python3
"""Analyze ensemble panel coverage and optionally run targeted ESGF searches for gaps."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

from rich.console import Console
from rich.table import Table

from esgpull.esgpullplus import config, fileops
from esgpull.esgpullplus.ensemble_panel import (
    DEFAULT_PANEL_MODELS,
    REQUIRED_MODEL,
    TARGET_MODELS,
    analyze_ensemble_gaps,
    analyze_processing_gaps,
    build_gap_search_jobs,
    coverage_pivot,
    write_gap_jobs_csv,
    write_gap_search_yamls,
    write_panel_search_yaml,
    write_ph_talk_historical_search_yaml,
    write_processing_gaps_csv,
    write_supplement_search_yaml,
)
from esgpull.esgpullplus.search import SearchResults


def _print_gap_summary(console: Console, summary, gaps) -> None:
    pivot = coverage_pivot(summary)
    table = Table(title="Ensemble panel coverage (target models per variable/scenario)")
    table.add_column("variable")
    table.add_column("experiment")
    table.add_column("period")
    table.add_column("n_selected")
    table.add_column("n_available")
    table.add_column(f"has_{REQUIRED_MODEL}")
    for (variable, experiment), row in pivot.iterrows():
        table.add_row(
            variable,
            experiment,
            str(row["period"]),
            str(int(row["n_models_selected"])),
            str(int(row["n_models_available"])),
            "yes" if row["has_required"] else "no",
        )
    console.print(table)

    missing_raw = [g for g in gaps if g.status == "missing_raw"]
    missing_regrid = [g for g in gaps if g.status == "missing_regridded"]
    console.print(
        f"\nGaps: {len(missing_raw)} missing raw downloads, "
        f"{len(missing_regrid)} need regrid only"
    )
    if missing_raw:
        console.print("\nMissing raw (first 15):")
        for gap in missing_raw[:15]:
            console.print(
                f"  {gap.variable} / {gap.experiment} / {gap.model} "
                f"({gap.period[0]}-{gap.period[1]})"
            )
        if len(missing_raw) > 15:
            console.print(f"  ... and {len(missing_raw) - 15} more")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--data-dir",
        type=Path,
        default=Path("/maps/rt582/data"),
        help="CMIP6 root (default: /maps/rt582/data)",
    )
    parser.add_argument(
        "--regridded-dir",
        type=Path,
        default=Path("/maps/rt582/esgf-download/data/nishant_data"),
        help="Directory with regridded crop NetCDF files",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("/maps/rt582/esgf-download/data/nishant_data"),
        help="Where to write ensemble_gaps.csv and panel yaml",
    )
    parser.add_argument(
        "--target",
        type=int,
        default=TARGET_MODELS,
        help=f"Models per variable/scenario (default: {TARGET_MODELS})",
    )
    parser.add_argument(
        "--write-yaml",
        action="store_true",
        help="Write search_ensemble_panel.yaml to repo root",
    )
    parser.add_argument(
        "--search-gaps",
        action="store_true",
        help="Run targeted ESGF searches for missing raw files (uses network)",
    )
    parser.add_argument(
        "--write-gap-yamls",
        action="store_true",
        help="Write targeted search YAMLs for missing raw files",
    )
    args = parser.parse_args()
    console = Console()

    summary, gaps = analyze_ensemble_gaps(
        data_dir=args.data_dir,
        regridded_dir=args.regridded_dir,
        panel=DEFAULT_PANEL_MODELS,
        target=args.target,
        required=REQUIRED_MODEL,
    )
    _print_gap_summary(console, summary, gaps)

    gaps_csv = args.output_dir / "ensemble_gaps.csv"
    write_gap_jobs_csv(gaps, gaps_csv)
    console.print(f"\nWrote {gaps_csv}")

    processing = analyze_processing_gaps(
        data_dir=args.data_dir,
        regridded_dir=args.regridded_dir,
        panel=DEFAULT_PANEL_MODELS,
        target=args.target,
        required=REQUIRED_MODEL,
    )
    processing_csv = args.output_dir / "processing_gaps.csv"
    write_processing_gaps_csv(processing, processing_csv)
    console.print(f"Wrote {processing_csv} ({len(processing)} actionable rows)")
    if not processing.empty:
        regrid_rows = processing[processing["action"] == "regrid"]
        if not regrid_rows.empty:
            console.print("\nRegrid needed (first 10):")
            for _, row in regrid_rows.head(10).iterrows():
                console.print(
                    f"  {row['variable']}/{row['experiment']}/{row['model']} "
                    f"({row['status']})"
                )

    if args.write_gap_yamls:
        gap_yaml_dir = config.repo_dir / "search_gaps"
        written = write_gap_search_yamls(
            gaps,
            gap_yaml_dir,
            base_yaml=config.repo_dir / "search.yaml",
        )
        for path in written:
            console.print(f"Wrote {path}")
        if written:
            console.print(
                "\nDownload a gap with:\n"
                "  esgplus download --config search_gaps/<file>.yaml"
            )

    if args.write_yaml:
        yaml_path = config.repo_dir / "search_ensemble_panel.yaml"
        write_panel_search_yaml(
            yaml_path,
            DEFAULT_PANEL_MODELS,
            base_yaml=config.repo_dir / "search.yaml",
        )
        write_supplement_search_yaml(
            config.repo_dir / "search_ensemble_supplement_ssp370.yaml",
            base_yaml=config.repo_dir / "search.yaml",
        )
        write_ph_talk_historical_search_yaml(
            config.repo_dir / "search_ensemble_supplement_ph_talk.yaml",
            base_yaml=config.repo_dir / "search.yaml",
        )
        console.print(f"Wrote {yaml_path}")
        console.print(f"Wrote {config.repo_dir / 'search_ensemble_supplement_ssp370.yaml'}")
        console.print(f"Wrote {config.repo_dir / 'search_ensemble_supplement_ph_talk.yaml'}")

    if args.search_gaps:
        jobs = build_gap_search_jobs(gaps)
        if not jobs:
            console.print("\nNo missing-raw gaps to search.")
            return
        base_cfg = fileops.read_yaml(config.repo_dir / "search.yaml") or {}
        meta = base_cfg.get("meta_criteria", {})
        console.print(f"\nRunning {len(jobs)} targeted gap searches...")
        for idx, job in enumerate(jobs, start=1):
            console.print(
                f"[{idx}/{len(jobs)}] {job['variable']} / "
                f"{job['experiment_id']} / {job['source_id']}"
            )
            sr = SearchResults(search_criteria=job, meta_criteria=meta)
            try:
                sr.run()
            except Exception as exc:
                console.print(f"  search failed: {exc}")
                continue
            n_files = len(sr.files) if hasattr(sr, "files") and sr.files else 0
            console.print(f"  found {n_files} files")


if __name__ == "__main__":
    main()
