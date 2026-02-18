#!/usr/bin/env python3
"""
Script to run ESGF search and analysis based on search.yaml configuration.

This script:
1. Reads search criteria from search.yaml
2. Performs the search
3. Analyzes source availability
4. Saves analysis_df.csv and optionally generates visualizations
"""

import argparse
from pathlib import Path
import sys
from rich.console import Console
from esgpull.esgpullplus import fileops, config
from esgpull.esgpullplus.search import SearchResults


def main():
    print(config.search_criteria_fp)
    parser = argparse.ArgumentParser(
        description="Run ESGF search and analysis from search.yaml"
    )
    parser.add_argument(
        "--config",
        type=str,
        default=str(config.search_criteria_fp),
        help="Path to search.yaml configuration file (default: search.yaml)",
    )
    parser.add_argument(
        "--output-dir",
        type=str,
        default=None,
        help="Output directory for analysis_df.csv and plots (default: notebooks/plots)",
    )
    parser.add_argument(
        "--show-plots",
        action="store_false",
        default=True,
        help="Display plots interactively (default: True)",
    )
    parser.add_argument(
        "--save-plots",
        action="store_true",
        default=True,
        help="Save plot images to output directory (default: True)",
    )
    parser.add_argument(
        "--require-both",
        action="store_true",
        default=True,
        help="Only analyze sources with both historical and SSP experiments (default: True)",
    )
    parser.add_argument(
        "--no-require-both",
        dest="require_both",
        action="store_false",
        help="Include sources even if they don't have both historical and SSP experiments",
    )
    parser.add_argument(
        "--config-path",
        dest="config_path",
        type=str,
        default=str(config.search_criteria_fp),
        help="Path to search.yaml configuration file (default: search.yaml)",
    )
    console = Console()

    start_time = fileops.print_timestamp(console=console, message="Starting search and analysis")
    args = parser.parse_args()
    # Read configuration from YAML
    config_path = Path(args.config_path)
    if not config_path.exists():
        print(f"Error: Configuration file not found: {config_path}")
        sys.exit(1)
    
    print(f"Reading configuration from {config_path}")
    config_dict = fileops.read_yaml(config_path)
    
    search_criteria = config_dict.get("search_criteria", {})
    meta_criteria = config_dict.get("meta_criteria", {})
    
    if not search_criteria:
        print("Error: No search_criteria found in configuration file")
        sys.exit(1)
    
    print(f"Search criteria: {search_criteria}")
    if meta_criteria:
        print(f"Meta criteria: {meta_criteria}")
    
    # set top_n to None to get all results
    search_criteria["filter"]["top_n"] = None
    search_criteria["filter"]["limit"] = None
    
    # Create SearchResults object
    search_results = SearchResults(
        search_criteria=search_criteria,
        meta_criteria=meta_criteria
    )
    
    # Perform search (with caching support)
    print("\nPerforming search (will use cache if available)...")
    try:
        # run() method handles caching: checks for cached results first,
        # loads if available, otherwise performs search and saves to cache
        search_results.run()
    except Exception as e:
        print(f"Error during search: {e}")
        sys.exit(1)
    
    # Analyze and visualize
    config.plots_dir.mkdir(parents=True, exist_ok=True)
    output_dir = Path(args.output_dir) if args.output_dir else config.plots_dir
    output_dir.mkdir(parents=True, exist_ok=True)
    
    print(f"\nAnalyzing source availability and saving to {output_dir}...")
    try:
        analysis_df, plots = search_results.analyze_and_visualize_sources(
            output_dir=str(output_dir),
            show_plots=args.show_plots,
            require_both=args.require_both,
            save_plots=args.save_plots,
        )
        
        print(f"\nAnalysis complete!")
        print(f"Found {len(analysis_df)} sources matching criteria")
        print(f"Analysis DataFrame saved to: {output_dir / 'analysis_df.csv'}")
        
        if plots:
            print(f"Plots saved to: {output_dir}")
            for plot_name, plot_path in plots.items():
                print(f"  - {plot_name}: {plot_path}")
        
    except Exception as e:
        print(f"Error during analysis: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

    end_time = fileops.print_timestamp(console=console, message="Ending search and analysis")
    processing_time = fileops.get_processing_time(start_time, end_time)
    fileops.print_timestamp(console=console, message=f"Processing time: {fileops.format_processing_time(processing_time)}")

if __name__ == "__main__":
    main()

