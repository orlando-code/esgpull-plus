"""
Example script demonstrating the source availability analysis functionality.

This script shows how to:
1. Search for datasets
2. Analyze which sources have both historical and SSP experiments
3. Visualize the results

Usage:
    python source_analysis_example.py
"""

from esgpull.esgpullplus.search import SearchResults
from pathlib import Path


def main():
    """Example usage of source availability analysis."""
    
    # Example search criteria - same format as CLI: esgpull search project:CMIP6 variable:tas ...
    # Uses dataset search (file=False) by default - faster than file search
    search_criteria = {
        "project": "CMIP6",
        "variable": "uo",  # CLI uses variable for dataset search
        "frequency": "mon",
        "table_id": "Omon",
        "experiment_id": "historical,ssp126,ssp245,ssp370,ssp585",
        # Don't filter by experiment_id - we want all experiments so the analysis
        # can find sources with both historical and SSP availability
    }
    
    meta_criteria = {
        "test": False,
        "filter": {"top_n": None, "limit": None},
        "data_dir": "/maps/rt582/data"
    }
    
    print("=" * 80)
    print("Source Availability Analysis Example")
    print("=" * 80)
    print("\nThis will search for datasets and analyze which sources")
    print("(institution_id, source_id) have both historical and SSP experiments.")
    print("\nSearch criteria:")
    for k, v in search_criteria.items():
        print(f"  {k}: {v}")
    print()
    
    # Create search results object
    search_results = SearchResults(
        search_criteria=search_criteria,
        meta_criteria=meta_criteria,
    )
    
    # Perform search (run() uses subsearches and caching, same as run_search_analysis)
    print("Performing search...")
    try:
        files = search_results.run()
        n = len(search_results.results_df) if search_results.results_df is not None else 0
        print(f"Found {n} datasets ({len(files)} rows after top_n/limit)")
    except Exception as e:
        print(f"Error during search: {e}")
        return
    
    if search_results.results_df is None or search_results.results_df.empty:
        print("No results found. Please adjust your search criteria.")
        return
    
    # Analyze source availability and create visualizations
    print("\n" + "=" * 80)
    print("Analyzing source availability and creating visualizations...")
    print("=" * 80)
    
    output_dir = Path("source_analysis_plots")
    import sys
    is_interactive = hasattr(sys.stdout, "isatty") and sys.stdout.isatty()
    analysis_df, saved_plots = search_results.analyze_and_visualize_sources(
        output_dir=str(output_dir),
        show_plots=is_interactive,
        historical_experiment="historical",
        ssp_pattern="ssp",
        require_both=True,  # Only show sources with both historical and SSP
    )
    
    if analysis_df.empty:
        print("No sources found matching criteria.")
        return
    
    print(f"\nFound {len(analysis_df)} sources with both historical and SSP experiments:")
    print()
    
    # Display summary
    for idx, row in analysis_df.iterrows():
        print(f"Source {idx + 1}: {row['institution_id']} / {row['source_id']}")
        print(f"  Historical experiments: {', '.join(row['historical_experiments'])}")
        print(f"  SSP experiments: {', '.join(row['ssp_experiments'])}")
        print(f"  Resolutions: {', '.join([f'{r:.2f}°' for r in row['resolutions']])}")
        print(f"  Ensemble members: {row['ensemble_count']}")
        print(f"  Total datasets: {row['total_datasets']}")
        print()
    
    if saved_plots:
        print("\nSaved plots:")
        for plot_name, plot_path in saved_plots.items():
            print(f"  {plot_name}: {plot_path}")
    
    # Save analysis results to CSV
    output_csv = output_dir / "source_availability_analysis.csv"
    output_csv.parent.mkdir(parents=True, exist_ok=True)
    
    # Flatten the DataFrame for CSV export (convert lists to strings)
    export_df = analysis_df.copy()
    for col in ["historical_experiments", "ssp_experiments", "resolutions", "ensemble_members"]:
        export_df[col] = export_df[col].apply(lambda x: ", ".join(map(str, x)) if isinstance(x, list) else str(x))
    export_df["resolution_counts"] = export_df["resolution_counts"].apply(str)
    
    export_df.to_csv(output_csv, index=False)
    print(f"\nAnalysis results saved to: {output_csv}")
    
    print("\n" + "=" * 80)
    print("Analysis complete!")
    print("=" * 80)


if __name__ == "__main__":
    main()

