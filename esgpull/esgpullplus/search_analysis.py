"""
Search result analysis and visualization for esgpullplus.

Provides:
- Source availability analysis (which models have both historical & SSP experiments)
- Symmetrical dataset identification (non-duplicate versions at each resolution)
- Resolution-aware ranking (higher resolution preferred)
- Visualization of source availability heatmaps, ensemble counts, and resolution distributions
"""

import logging
from pathlib import Path
from typing import Optional

import numpy as np
import pandas as pd

from esgpull.esgpullplus import utils

log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Analysis
# ---------------------------------------------------------------------------

def analyze_source_availability(
    results_df: pd.DataFrame,
    historical_experiment: str = "historical",
    ssp_pattern: str = "ssp",
    require_both: bool = True,
) -> pd.DataFrame:
    """
    Analyze which sources (institution_id, source_id) have datasets available
    for both historical and SSP experiments, grouped by variable.

    The returned DataFrame contains one row per (variable, institution_id, source_id)
    combination, with columns describing experiment coverage, resolution, and
    ensemble membership.

    Args:
        results_df: DataFrame of search results (must contain at least
            ``variable``, ``institution_id``, ``source_id``, ``experiment_id``,
            ``member_id``, ``nominal_resolution`` columns).
        historical_experiment: Experiment ID for historical runs.
        ssp_pattern: Prefix used to identify SSP experiments (e.g. ``"ssp"``
            matches ``ssp126``, ``ssp245``, etc.).
        require_both: If True, only return sources with **both** historical
            and at least one SSP experiment.

    Returns:
        DataFrame with analysis results.  Empty if no sources match.
    """
    if results_df is None or results_df.empty:
        raise ValueError("No results to analyze. Run search first.")

    # Ensure resolution column exists
    if "resolution" not in results_df.columns:  # TODO: resolution hasn't been calculated properly, since 'nominal_resolution' is a column of lists/strings
        results_df = results_df.copy()
        results_df["resolution"] = results_df.apply(
            lambda row: utils.calc_resolution(row.get("nominal_resolution", "") or ""),
            axis=1,
        )

    var_dfs = []
    for variable in results_df["variable"].unique():
        var_df = results_df[results_df["variable"] == variable]
        grouped = var_df.groupby(["institution_id", "source_id"])

        rows = []
        for (institution_id, source_id), group_df in grouped:
            experiments = group_df["experiment_id"].dropna().unique().tolist()

            historical_exps = [
                e for e in experiments
                if e.lower() == historical_experiment.lower()
            ]
            ssp_exps = [
                e for e in experiments
                if e.lower().startswith(ssp_pattern.lower())
            ]

            has_historical = len(historical_exps) > 0
            has_ssp = len(ssp_exps) > 0

            if require_both and not (has_historical and has_ssp):
                continue

            resolutions = sorted(
                r for r in group_df["resolution"].dropna().unique() if r < 9999.0
            )
            nominal_resolutions = group_df["nominal_resolution"].dropna().unique().tolist()
            resolution_counts = {
                k: int(v)
                for k, v in group_df["resolution"].value_counts().items()
                if k < 9999.0
            }

            ensemble_members = sorted(group_df["member_id"].dropna().unique().tolist())

            # Per-experiment ensemble members
            per_exp = {}
            standard_experiments = ["historical", "ssp126", "ssp245", "ssp370", "ssp585"]
            for exp_name in standard_experiments:
                matching = [e for e in experiments if e.lower() == exp_name.lower()]
                if matching:
                    members = sorted(
                        group_df[group_df["experiment_id"] == matching[0]]["member_id"]
                        .dropna().unique().tolist()
                    )
                    per_exp[f"{exp_name}_ensemble_members"] = members
                else:
                    per_exp[f"{exp_name}_ensemble_members"] = []

            # Non-standard experiments
            for exp_id in experiments:
                if exp_id.lower() not in [e.lower() for e in standard_experiments]:
                    col = f"{exp_id}_ensemble_members"
                    per_exp[col] = sorted(
                        group_df[group_df["experiment_id"] == exp_id]["member_id"]
                        .dropna().unique().tolist()
                    )

            row = {
                "variable": variable,
                "institution_id": institution_id,
                "source_id": source_id,
                "has_historical": has_historical,
                "has_ssp": has_ssp,
                "historical_experiments": sorted(historical_exps),
                "ssp_experiments": sorted(ssp_exps),
                "resolutions": resolutions,
                "nominal_resolutions": nominal_resolutions,
                "resolution_counts": resolution_counts,
                "ensemble_members": ensemble_members,
                "ensemble_count": len(ensemble_members),
                "total_datasets": len(group_df),
                "table_id": group_df["table_id"].dropna().unique().tolist(),
                "frequency": group_df["frequency"].dropna().unique().tolist(),
                "data_nodes": group_df["data_node"].dropna().unique().tolist(),
            }
            row.update(per_exp)
            rows.append(row)

        if rows:
            var_dfs.append(pd.DataFrame(rows))

    if not var_dfs:
        return pd.DataFrame()
    return pd.concat(var_dfs, ignore_index=True)


def summarize_symmetrical_datasets(
    results_df: pd.DataFrame,
    historical_experiment: str = "historical",
    ssp_pattern: str = "ssp",
) -> pd.DataFrame:
    """
    Focused summary answering: *how many non-duplicate versions of symmetrical
    datasets exist at each resolution, with higher-resolution preferred?*

    Returns a DataFrame sorted by resolution (ascending = higher res first) with
    columns:

    - variable, institution_id, source_id
    - nominal_resolution, resolution (numeric degrees)
    - n_experiments (how many distinct experiments)
    - n_ensemble_members (unique member_ids across all experiments)
    - n_versions (unique version strings)
    - n_files (total file count)
    - experiments (list)
    - symmetrical (bool - True if has both historical and >= 1 SSP)
    """
    if results_df is None or results_df.empty:
        raise ValueError("No results to summarize.")

    df = results_df.copy()
    if "resolution" not in df.columns:
        df["resolution"] = df.apply(
            lambda row: utils.calc_resolution(row.get("nominal_resolution", "") or ""),
            axis=1,
        )

    group_cols = ["variable", "institution_id", "source_id"]
    rows = []
    for keys, grp in df.groupby(group_cols):
        variable, inst, src = keys
        experiments = sorted(grp["experiment_id"].dropna().unique())
        has_hist = any(e.lower() == historical_experiment.lower() for e in experiments)
        has_ssp = any(e.lower().startswith(ssp_pattern.lower()) for e in experiments)

        # Best (smallest) resolution for this source
        valid_res = grp.loc[grp["resolution"] < 9999.0, "resolution"]
        best_res = valid_res.min() if not valid_res.empty else 9999.0
        nominal = grp.loc[grp["resolution"] == best_res, "nominal_resolution"]
        nom_str = nominal.iloc[0] if not nominal.empty else ""

        n_versions = grp["version"].dropna().nunique() if "version" in grp.columns else 0
        n_members = grp["member_id"].dropna().nunique() if "member_id" in grp.columns else 0

        rows.append({
            "variable": variable,
            "institution_id": inst,
            "source_id": src,
            "nominal_resolution": nom_str,
            "resolution": best_res,
            "n_experiments": len(experiments),
            "n_ensemble_members": n_members,
            "n_versions": n_versions,
            "n_files": len(grp),
            "experiments": experiments,
            "symmetrical": has_hist and has_ssp,
        })

    summary = pd.DataFrame(rows)
    if summary.empty:
        return summary

    # Sort: symmetrical first, then by resolution ascending (better first)
    summary = summary.sort_values(
        by=["symmetrical", "resolution"],
        ascending=[False, True],
    ).reset_index(drop=True)
    return summary


def get_experiments_by_source(
    results_df: pd.DataFrame,
    variable: Optional[str] = None,
) -> pd.DataFrame:
    """
    Show what experiments are found for each source, before any filtering.

    Args:
        results_df: DataFrame of search results.
        variable: Optional variable to filter by.

    Returns:
        DataFrame with variable, institution_id, source_id, experiment_id,
        ensemble_members, ensemble_count, dataset_count.
    """
    if results_df is None or results_df.empty:
        raise ValueError("No results to analyze.")

    df = results_df.copy()
    if variable:
        df = df[df["variable"] == variable]

    rows = []
    for (var, inst, src, exp), grp in df.groupby(
        ["variable", "institution_id", "source_id", "experiment_id"]
    ):
        members = sorted(grp["member_id"].dropna().unique().tolist())
        rows.append({
            "variable": var,
            "institution_id": inst,
            "source_id": src,
            "experiment_id": exp,
            "ensemble_members": members,
            "ensemble_count": len(members),
            "dataset_count": len(grp),
        })
    return pd.DataFrame(rows)


# ---------------------------------------------------------------------------
# Visualization
# ---------------------------------------------------------------------------


def visualize_source_availability(
    analysis_df: pd.DataFrame,
    output_dir: Optional[str] = None,
    show_plots: bool = True,
    save_plots: bool = True,
) -> dict:
    """
    Create visualizations of source availability analysis.

    Produces four plots:
    1. Heatmap of sources vs experiments
    2. Bar chart of ensemble member counts per source
    3. Resolution distribution per source
    4. Summary table

    Args:
        analysis_df: DataFrame from :func:`analyze_source_availability`.
        output_dir: Directory to save plots (None = don't save).
        show_plots: Whether to call ``plt.show()``.
        save_plots: Whether to save plots to *output_dir*.

    Returns:
        Dictionary mapping plot name to saved file path.
    """
    try:
        import matplotlib.pyplot as plt
        import seaborn as sns
    except ImportError:
        raise ImportError(
            "matplotlib and seaborn are required for visualizations. "
            "Install with: pip install matplotlib seaborn"
        )

    if analysis_df.empty:
        log.warning("No sources found matching criteria - nothing to visualize.")
        return {}

    sns.set_style("whitegrid")
    plt.rcParams["figure.figsize"] = (14, 8)

    saved_plots: dict[str, str] = {}

    # Helper -----------------------------------------------------------------
    def _save(fig, name):
        if output_dir and save_plots:
            p = Path(output_dir) / f"{name}.png"
            p.parent.mkdir(parents=True, exist_ok=True)
            fig.savefig(p, dpi=300, bbox_inches="tight")
            saved_plots[name] = str(p)
        if show_plots:
            plt.show()
        else:
            plt.close(fig)

    source_labels = [
        f"{row['institution_id']}\n{row['source_id']}"
        for _, row in analysis_df.iterrows()
    ]

    # 1. Heatmap: sources vs experiments ------------------------------------
    all_experiments = set()
    for _, row in analysis_df.iterrows():
        all_experiments.update(row["historical_experiments"])
        all_experiments.update(row["ssp_experiments"])
    all_experiments = sorted(all_experiments)

    heatmap_data = []
    for _, row in analysis_df.iterrows():
        heatmap_data.append([
            1 if exp in row["historical_experiments"] or exp in row["ssp_experiments"] else 0
            for exp in all_experiments
        ])

    fig, ax = plt.subplots(figsize=(16, max(8, len(analysis_df) * 0.4)))
    heatmap_df = pd.DataFrame(heatmap_data, index=source_labels, columns=all_experiments)
    sns.heatmap(heatmap_df, annot=True, fmt="d", cmap="YlOrRd",
                cbar_kws={"label": "Available"}, ax=ax, linewidths=0.5)
    ax.set_title("Source Availability: Historical and SSP Experiments", fontsize=14, fontweight="bold")
    ax.set_xlabel("Experiment ID", fontsize=12)
    ax.set_ylabel("Institution / Source", fontsize=12)
    plt.xticks(rotation=45, ha="right")
    plt.yticks(rotation=0)
    plt.tight_layout()
    _save(fig, "source_availability_heatmap")

    # 2. Bar chart: ensemble counts -----------------------------------------
    fig, ax = plt.subplots(figsize=(14, max(8, len(analysis_df) * 0.4)))
    # order analysis_df by ensemble_count descending
    analysis_df = analysis_df.sort_values(by="ensemble_count", ascending=True)
    counts = analysis_df["ensemble_count"].values
    bars = ax.barh(range(len(analysis_df)), counts, color="steelblue")
    for i, (bar, c) in enumerate(zip(bars, counts)):
        ax.text(c + max(counts) * 0.01, i, str(c), va="center", fontweight="bold")
    ax.set_yticks(range(len(analysis_df)))
    ax.set_yticklabels(source_labels)
    ax.set_xlabel("Number of Ensemble Members", fontsize=12)
    ax.set_title("Ensemble Member Count per Source", fontsize=14, fontweight="bold")
    ax.grid(axis="x", alpha=0.3)
    plt.tight_layout()
    _save(fig, "ensemble_counts")

    # 3. Resolution distribution as a heatmap (block table) by source and resolution ---
    import numpy as np
    from matplotlib.colors import LogNorm
    # order by ascending smallest resolution first
    analysis_df = analysis_df.sort_values(by="resolutions", ascending=True)
    all_res = set()
    for _, row in analysis_df.iterrows():
        all_res.update(row["resolutions"])
    all_res = sorted(all_res)
    if all_res:
        # Build a matrix of (n_sources x n_resolutions) with dataset counts
        resolution_labels = [f"{res:.2f}°" for res in all_res]
        source_count = len(analysis_df)
        res_count = len(all_res)
        data_matrix = np.zeros((source_count, res_count), dtype=int)
        # make sure that within all_res rows are sorted by number of counts (with highest counts first)
        analysis_df = analysis_df.sort_values(by="resolution_counts", ascending=False)
        for i, (_, row) in enumerate(analysis_df.iterrows()):
            for j, res in enumerate(all_res):
                data_matrix[i, j] = row["resolution_counts"].get(res, 0)
        # If all zeros, fall back to message
        if np.count_nonzero(data_matrix) == 0:
            fig, ax = plt.subplots(figsize=(max(8, source_count * 0.4), 6))
            ax.text(0.5, 0.5, "No resolution data available", ha="center", va="center",
                    fontsize=14, transform=ax.transAxes)
            ax.axis("off")
        else:
            fig, ax = plt.subplots(figsize=(max(10, res_count * 1.2), max(8, source_count * 0.4)))
            # Choose log scale if needed (avoid log(0)): add small offset for display, handle vmin
            mask = data_matrix > 0
            # To avoid log(0), set minimum to 1 for the colormap, zeros remain white
            plot_data = np.where(mask, data_matrix, np.nan)
            cmap = sns.color_palette("YlOrRd", as_cmap=True)
            norm = LogNorm(vmin=1, vmax=np.nanmax(plot_data)) if np.nanmax(plot_data) >= 10 else None
            # Draw heatmap with integer annotation
            sns.heatmap(
                plot_data,
                ax=ax,
                annot=data_matrix,
                fmt="d",
                cmap=cmap,
                cbar=True,
                linewidths=0.5,
                linecolor="grey",
                square=False,
                mask=~mask,
                norm=norm,
                cbar_kws={
                    "label": "Number of Datasets",
                    "format": "%d",
                    'ticks': [int(x) for x in np.unique(data_matrix[mask]) if x > 0]
                } if norm else {"label": "Number of Datasets"}
            )
            ax.set_xticklabels(resolution_labels, rotation=45, ha="right")
            ax.set_yticklabels(source_labels, rotation=0, ha="right")
            ax.set_xlabel("Resolution (degrees)", fontsize=12)
            ax.set_ylabel("Institution / Source", fontsize=12)
            ax.set_title("Datasets by Source and Resolution", fontsize=14, fontweight="bold")
            # Force colorbar ticks to only integer values (for log colorbar)
            if norm:
                cbar = ax.collections[0].colorbar
                cbar.set_ticks([int(t) for t in np.unique(data_matrix[mask]) if t > 0])
                cbar.set_ticklabels([str(int(t)) for t in np.unique(data_matrix[mask]) if t > 0])
                cbar.ax.tick_params(labelsize=10)
        plt.tight_layout()
        _save(fig, "resolution_distribution")
    else:
        fig, ax = plt.subplots(figsize=(max(8, len(analysis_df) * 0.4), 6))
        ax.text(0.5, 0.5, "No resolution data available", ha="center", va="center",
                fontsize=14, transform=ax.transAxes)
        ax.axis("off")
        plt.tight_layout()
        _save(fig, "resolution_distribution")

    # 4. Summary table -------------------------------------------------------
    fig, ax = plt.subplots(figsize=(16, max(6, len(analysis_df) * 0.3)))
    ax.axis("tight")
    ax.axis("off")
    table_data = []
    for _, row in analysis_df.iterrows():
        table_data.append([
            row["institution_id"],
            row["source_id"],
            ", ".join(row["historical_experiments"]) or "None",
            ", ".join(row["ssp_experiments"]) or "None",
            f"{len(row['resolutions'])} unique",
            f"{row['ensemble_count']} members",
            f"{row['total_datasets']} datasets",
        ])
    tbl = ax.table(
        cellText=table_data,
        colLabels=["Institution", "Source", "Historical Exps", "SSP Exps",
                    "Resolutions", "Ensembles", "Total Datasets"],
        cellLoc="left", loc="center",
        colWidths=[0.15, 0.15, 0.2, 0.2, 0.1, 0.1, 0.1],
    )
    tbl.auto_set_font_size(False)
    tbl.set_fontsize(9)
    tbl.scale(1, 2)
    for i in range(7):
        tbl[(0, i)].set_facecolor("#4CAF50")
        tbl[(0, i)].set_text_props(weight="bold", color="white")
    # ax.set_title("Source Availability Summary", fontdict={"fontsize": 14, "fontweight": "bold", "verticalalignment": "top"})
    plt.tight_layout()
    _save(fig, "source_summary_table")
    print("Saved plots to: ", output_dir)
    return saved_plots


def analyze_and_visualize(
    results_df: pd.DataFrame,
    output_dir: Optional[str] = None,
    show_plots: bool = True,
    save_plots: bool = True,
    historical_experiment: str = "historical",
    ssp_pattern: str = "ssp",
    require_both: bool = True,
) -> tuple[pd.DataFrame, dict]:
    """
    Convenience wrapper: run analysis then visualize.

    Args:
        results_df: DataFrame of search results.
        output_dir: Directory to save plots and analysis CSV.
        show_plots: Whether to display plots interactively.
        save_plots: Whether to write plot PNGs to *output_dir*.
        historical_experiment: Experiment ID for historical runs.
        ssp_pattern: Prefix for SSP experiments.
        require_both: Only include sources with both historical and SSP.

    Returns:
        Tuple of (analysis_df, dict of saved plot paths).
    """
    log.info("Analyzing source availability...")
    analysis_df = analyze_source_availability(
        results_df,
        historical_experiment=historical_experiment,
        ssp_pattern=ssp_pattern,
        require_both=require_both,
    )

    if analysis_df.empty:
        log.warning("No sources found matching criteria.")
        return analysis_df, {}

    log.info(f"Found {len(analysis_df)} sources matching criteria.")

    if output_dir:
        out = Path(output_dir)
        out.mkdir(parents=True, exist_ok=True)
        analysis_df.to_csv(out / "analysis_df.csv", index=False)

    saved_plots = visualize_source_availability(
        analysis_df=analysis_df,
        output_dir=output_dir,
        show_plots=show_plots,
        save_plots=save_plots,
    )
    return analysis_df, saved_plots
