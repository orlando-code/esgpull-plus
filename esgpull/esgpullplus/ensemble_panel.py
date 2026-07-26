"""Helpers for building a fixed multi-model ensemble panel (up to N models per variable/scenario)."""

from __future__ import annotations

import json
import re
from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, Optional

import numpy as np
import pandas as pd
import xarray as xr

from esgpull.esgpullplus import fileops

VARIABLES = ("tos", "ph", "talk")
EXPERIMENTS = ("historical", "ssp245", "ssp370")
PERIOD_YEARS = {
    "historical": (2005, 2014),
    "ssp245": (2091, 2100),
    "ssp370": (2091, 2100),
}
REQUIRED_MODEL = "GFDL-ESM4"
TARGET_MODELS = 20
FILE_PERIOD_RE = re.compile(r"_(\d{6})-(\d{6})")
GRID_TAG_RE = re.compile(r"_(gn|gr)_")
LEVEL_DIMS = ("lev", "olevel", "level", "depth", "deptht", "z")

# Current tos/historical panel (2005-2014) from regridded nishant outputs.
DEFAULT_PANEL_MODELS: tuple[str, ...] = (
    "AWI-CM-1-1-MR",
    "AWI-ESM-1-1-LR",
    "BCC-CSM2-MR",
    "BCC-ESM1",
    "CAMS-CSM1-0",
    "CMCC-CM2-HR4",
    "CMCC-CM2-SR5",
    "CMCC-ESM2",
    "CanESM5",
    "EC-Earth3-AerChem",
    "EC-Earth3-CC",
    "EC-Earth3-HR",
    "EC-Earth3-Veg-LR",
    "FGOALS-f3-L",
    "FGOALS-g3",
    "GFDL-CM4",
    "GFDL-ESM4",
    "ICON-ESM-LR",
    "MPI-ESM1-2-HR",
    "TaiESM1",
)


@dataclass(frozen=True)
class EnsembleGap:
    variable: str
    experiment: str
    model: str
    period: tuple[int, int]
    status: str  # missing_raw | missing_regridded | missing_on_esgf (manual)

    @property
    def key(self) -> tuple[str, str, str]:
        return self.variable, self.experiment, self.model


def parse_regridded_name(path: Path) -> Optional[tuple[str, str, str]]:
    stem = path.name
    if "_regridded_" not in stem:
        return None
    stem = re.sub(r"_regridded(?:_crop_[^.]+)?\.nc$", "", stem)
    stem = re.sub(r"_top_level$", "", stem)
    parts = stem.split("_")
    if len(parts) < 4 or parts[1] != "Omon" or parts[0] not in VARIABLES:
        return None
    for idx, part in enumerate(parts):
        if part in EXPERIMENTS:
            return parts[0], "_".join(parts[2:idx]), part
    return None


def file_period_overlaps(path: Path, y0: int, y1: int) -> bool:
    match = FILE_PERIOD_RE.search(path.name)
    if not match:
        return False
    start_year = int(match.group(1)[:4])
    end_year = int(match.group(2)[:4])
    return start_year <= y1 and end_year >= y0


def index_regridded_files(regridded_dir: Path) -> dict[tuple[str, str, str], list[Path]]:
    out: dict[tuple[str, str, str], list[Path]] = defaultdict(list)
    for path in regridded_dir.glob("*.nc"):
        parsed = parse_regridded_name(path)
        if parsed:
            out[parsed].append(path)
    return out


def index_raw_cmip6_files(data_dir: Path) -> dict[tuple[str, str, str], list[Path]]:
    """Index raw CMIP6 Omon files under data_dir/CMIP6."""
    out: dict[tuple[str, str, str], list[Path]] = defaultdict(list)
    cmip6_root = data_dir / "CMIP6"
    if not cmip6_root.exists():
        return out
    for path in cmip6_root.rglob("*.nc"):
        name = path.name
        if any(x in name for x in ("regridded", "chunk", "seafloor", "top_level")):
            continue
        match = re.match(
            r"^(tos|ph|talk)_Omon_(.+)_(historical|ssp245|ssp370)_r1i1p1f1_(gn|gr)_(\d{6}-\d{6})\.nc$",
            name,
        )
        if not match:
            continue
        var, model, exp = match.group(1), match.group(2), match.group(3)
        out[(var, model, exp)].append(path)
    return out


def has_period_coverage(
    files: Iterable[Path],
    y0: int,
    y1: int,
) -> bool:
    return any(file_period_overlaps(path, y0, y1) for path in files)


def parse_file_period(path: Path) -> tuple[tuple[int, int], tuple[int, int]]:
    match = FILE_PERIOD_RE.search(path.name)
    if not match:
        raise ValueError(f"No period found in {path.name}")
    start, end = match.group(1), match.group(2)
    return (int(start[:4]), int(start[4:])), (int(end[:4]), int(end[4:]))


def covered_years(files: Iterable[Path], y0: int, y1: int) -> set[int]:
    years: set[int] = set()
    for path in files:
        if not file_period_overlaps(path, y0, y1):
            continue
        (sy, _), (ey, _) = parse_file_period(path)
        for year in range(max(sy, y0), min(ey, y1) + 1):
            years.add(year)
    return years


def has_full_period_coverage(files: Iterable[Path], y0: int, y1: int) -> bool:
    return covered_years(files, y0, y1) >= set(range(y0, y1 + 1))


def _grid_tag(path: Path) -> str:
    match = GRID_TAG_RE.search(path.name)
    return match.group(1) if match else "unknown"


def pick_regridded_files(files: Iterable[Path], y0: int, y1: int) -> list[Path]:
    """Choose one grid variant and prefer surface-only (_top_level) regridded files."""
    overlapping = [path for path in files if file_period_overlaps(path, y0, y1)]
    if not overlapping:
        return []

    top_level = [path for path in overlapping if "_top_level" in path.name]
    if top_level:
        overlapping = top_level

    by_grid: dict[str, list[Path]] = defaultdict(list)
    for path in overlapping:
        by_grid[_grid_tag(path)].append(path)

    def grid_score(group: list[Path]) -> tuple[int, int, int]:
        years = covered_years(group, y0, y1)
        full = int(years >= set(range(y0, y1 + 1)))
        return (full, len(years), len(group))

    ranked = sorted(by_grid.items(), key=lambda item: (grid_score(item[1]), item[0] != "gn"), reverse=True)
    return sorted(ranked[0][1])


def assign_time_if_needed(ds: xr.Dataset, path: Path) -> xr.Dataset:
    if ds.time.attrs.get("units") or np.issubdtype(ds.time.dtype, np.datetime64):
        return ds
    (sy, sm), (ey, em) = parse_file_period(path)
    times = pd.date_range(
        pd.Timestamp(sy, sm, 1),
        pd.Timestamp(ey, em, 1),
        periods=ds.sizes["time"],
    )
    return ds.assign_coords(time=times)


def extract_surface_level(da: xr.DataArray) -> xr.DataArray:
    for dim in LEVEL_DIMS:
        if dim in da.dims:
            da = da.isel({dim: 0}, drop=True)
            break
    return da.where(np.abs(da) < 1e19)


def sel_year_range(da: xr.DataArray, y0: int, y1: int) -> xr.DataArray:
    years = da.time.dt.year
    return da.isel(time=(years >= y0) & (years <= y1))


def load_model_period(
    files: list[Path],
    var: str,
    y0: int,
    y1: int,
) -> tuple[Optional[xr.DataArray], Optional[xr.DataArray], bool, float]:
    """Return period-mean field, domain-mean time series, full-period flag, and grid resolution."""
    selected = pick_regridded_files(files, y0, y1)
    if not selected:
        return None, None, False, float("nan")

    chunks: list[xr.DataArray] = []
    for path in selected:
        with assign_time_if_needed(xr.open_dataset(path, decode_times=True), path) as ds:
            chunks.append(extract_surface_level(ds[var]))
    da = xr.concat(chunks, dim="time").sortby("time")
    da = da.groupby("time").mean()
    da = sel_year_range(da, y0, y1)
    if da.sizes.get("time", 0) == 0:
        return None, None, False, float("nan")

    level_dims_left = [dim for dim in LEVEL_DIMS if dim in da.dims]
    if level_dims_left:
        raise ValueError(
            f"{var} still has level dims {level_dims_left} after surface extraction "
            f"(from {[p.name for p in selected]})"
        )

    full_period = has_full_period_coverage(selected, y0, y1)
    res = float(abs(da.lon.diff("lon").median()))
    field = da.mean("time", skipna=True)
    ts = da.mean(["lat", "lon"], skipna=True)
    return field, ts, full_period, res


def normalize_monthly_timeseries(ts: xr.DataArray) -> xr.DataArray:
    """Collapse to one value per calendar month on a shared YYYYMM coordinate."""
    ym = ts.time.dt.year * 100 + ts.time.dt.month
    monthly = ts.groupby(ym).mean()
    return monthly.rename({monthly.dims[0]: "month"})


def ensemble_mean_timeseries(model_ts: dict[str, xr.DataArray]) -> xr.DataArray:
    """Multimodel mean of domain-mean monthly time series aligned by calendar month."""
    if not model_ts:
        return xr.DataArray()
    normalized = [normalize_monthly_timeseries(ts) for ts in model_ts.values()]
    stacked = xr.concat(normalized, dim="model", join="outer")
    stacked = stacked.assign_coords(model=list(model_ts.keys()))
    return stacked.mean("model", skipna=True)


def month_coord_to_decimal_year(month: xr.DataArray) -> np.ndarray:
    """Map YYYYMM month codes to decimal years for plotting."""
    return (month // 100).values + ((month % 100).values - 1) / 12.0


def models_with_coverage(
    index: dict[tuple[str, str, str], list[Path]],
    variable: str,
    experiment: str,
    y0: int,
    y1: int,
) -> list[str]:
    models = []
    for (var, model, exp), paths in index.items():
        if var == variable and exp == experiment and has_period_coverage(paths, y0, y1):
            models.append(model)
    return sorted(set(models))


def select_models_for_slot(
    variable: str,
    experiment: str,
    panel: Iterable[str],
    raw_index: dict[tuple[str, str, str], list[Path]],
    regrid_index: dict[tuple[str, str, str], list[Path]],
    *,
    target: int = TARGET_MODELS,
    required: str = REQUIRED_MODEL,
) -> list[str]:
    """Pick up to ``target`` models: panel first, then fill from raw availability."""
    y0, y1 = PERIOD_YEARS[experiment]
    selected: list[str] = []

    def add(model: str) -> None:
        if model in selected or len(selected) >= target:
            return
        raw_ok = has_period_coverage(raw_index.get((variable, model, experiment), []), y0, y1)
        regrid_ok = has_period_coverage(
            regrid_index.get((variable, model, experiment), []), y0, y1
        )
        if raw_ok or regrid_ok:
            selected.append(model)

    if required:
        add(required)
    for model in panel:
        add(model)

    if len(selected) < target:
        extras = sorted(
            set(
                models_with_coverage(raw_index, variable, experiment, y0, y1)
                + models_with_coverage(regrid_index, variable, experiment, y0, y1)
            ),
            key=lambda model: (
                has_full_period_coverage(
                    regrid_index.get((variable, model, experiment), []), y0, y1
                ),
                has_full_period_coverage(
                    raw_index.get((variable, model, experiment), []), y0, y1
                ),
                model,
            ),
            reverse=True,
        )
        for model in extras:
            add(model)
            if len(selected) >= target:
                break
    return selected[:target]


def analyze_ensemble_gaps(
    *,
    data_dir: Path,
    regridded_dir: Path,
    panel: Iterable[str] = DEFAULT_PANEL_MODELS,
    target: int = TARGET_MODELS,
    required: str = REQUIRED_MODEL,
) -> tuple[pd.DataFrame, list[EnsembleGap]]:
    """Return coverage summary and download/regrid gaps for the fixed panel strategy."""
    raw_index = index_raw_cmip6_files(data_dir)
    regrid_index = index_regridded_files(regridded_dir)

    rows: list[dict] = []
    gaps: list[EnsembleGap] = []

    for variable in VARIABLES:
        for experiment in EXPERIMENTS:
            y0, y1 = PERIOD_YEARS[experiment]
            chosen = select_models_for_slot(
                variable,
                experiment,
                panel,
                raw_index,
                regrid_index,
                target=target,
                required=required,
            )
            for model in chosen:
                raw_ok = has_period_coverage(
                    raw_index.get((variable, model, experiment), []), y0, y1
                )
                regrid_ok = has_period_coverage(
                    regrid_index.get((variable, model, experiment), []), y0, y1
                )
                rows.append(
                    {
                        "variable": variable,
                        "experiment": experiment,
                        "model": model,
                        "period": f"{y0}-{y1}",
                        "has_raw": raw_ok,
                        "has_regridded": regrid_ok,
                        "in_panel": model in set(panel),
                        "required_model": model == required,
                    }
                )
                if regrid_ok:
                    continue
                if raw_ok:
                    gaps.append(
                        EnsembleGap(
                            variable=variable,
                            experiment=experiment,
                            model=model,
                            period=(y0, y1),
                            status="missing_regridded",
                        )
                    )
                else:
                    gaps.append(
                        EnsembleGap(
                            variable=variable,
                            experiment=experiment,
                            model=model,
                            period=(y0, y1),
                            status="missing_raw",
                        )
                    )

            rows.append(
                {
                    "variable": variable,
                    "experiment": experiment,
                    "model": "__summary__",
                    "period": f"{y0}-{y1}",
                    "has_raw": None,
                    "has_regridded": None,
                    "in_panel": None,
                    "required_model": None,
                    "n_models_selected": len(chosen),
                    "n_models_available": len(
                        sorted(
                            set(
                                models_with_coverage(raw_index, variable, experiment, y0, y1)
                                + models_with_coverage(
                                    regrid_index, variable, experiment, y0, y1
                                )
                            )
                        )
                    ),
                    "has_required": required in chosen,
                }
            )

    summary = pd.DataFrame(rows)
    gaps = _ensure_required_model_gaps(gaps, raw_index, regrid_index, required)
    return summary, gaps


def _ensure_required_model_gaps(
    gaps: list[EnsembleGap],
    raw_index: dict[tuple[str, str, str], list[Path]],
    regrid_index: dict[tuple[str, str, str], list[Path]],
    required: str,
) -> list[EnsembleGap]:
    """Always track missing required-model coverage, even if not in the selected panel."""
    seen = {gap.key for gap in gaps}
    for variable in VARIABLES:
        for experiment in EXPERIMENTS:
            y0, y1 = PERIOD_YEARS[experiment]
            raw_ok = has_period_coverage(
                raw_index.get((variable, required, experiment), []), y0, y1
            )
            regrid_ok = has_period_coverage(
                regrid_index.get((variable, required, experiment), []), y0, y1
            )
            if regrid_ok:
                continue
            key = (variable, experiment, required)
            if key in seen:
                continue
            gaps.append(
                EnsembleGap(
                    variable=variable,
                    experiment=experiment,
                    model=required,
                    period=(y0, y1),
                    status="missing_regridded" if raw_ok else "missing_raw",
                )
            )
    return gaps


def build_supplement_search_criteria(
    *,
    variables: Iterable[str],
    experiments: Iterable[str],
    top_n: int = 25,
    base_criteria: Optional[dict] = None,
) -> dict:
    """Broad ESGF discovery when the local catalog cannot reach the target panel size."""
    criteria = dict(base_criteria or {})
    criteria.update(
        {
            "project": "CMIP6",
            "table_id": "Omon",
            "experiment_id": ",".join(experiments),
            "variable": ",".join(variables),
            "grid_label": criteria.get("grid_label", "gn,gr"),
            "member_id": criteria.get("member_id", "r1i1p1f1"),
        }
    )
    criteria.pop("source_id", None)
    filt = dict(criteria.get("filter") or {})
    filt["top_n"] = top_n
    filt.setdefault("limit", 2000)
    criteria["filter"] = filt
    return criteria


def write_supplement_search_yaml(
    path: Path,
    *,
    base_yaml: Optional[Path] = None,
) -> Path:
    """Write yaml for under-filled slots: ph/talk historical + all ssp370 variables."""
    import yaml

    base = {}
    meta = {
        "test": False,
        "data_dir": "/maps/rt582/data",
        "max_workers": 32,
        "delete_original": False,
        "verbose": False,
        "find_alternatives": True,
        "historic_start_year": 2000,
        "historic_end_year": 2014,
        "future_start_year": 2090,
        "future_end_year": 2100,
    }
    if base_yaml and base_yaml.exists():
        loaded = fileops.read_yaml(base_yaml)
        base = loaded.get("search_criteria", {})
        meta = loaded.get("meta_criteria", meta)

    payload = {
        "search_criteria": build_supplement_search_criteria(
            variables=VARIABLES,
            experiments=("ssp370",),
            top_n=25,
            base_criteria=base,
        ),
        "meta_criteria": meta,
        "_note": (
            "Run this after search_ensemble_panel.yaml to discover additional ssp370 models. "
            "Also run search_ensemble_supplement_ph_talk.yaml for historical ph/talk."
        ),
    }
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w") as handle:
        yaml.safe_dump(payload, handle, sort_keys=False, default_flow_style=False)
    return path


def write_ph_talk_historical_search_yaml(
    path: Path,
    *,
    base_yaml: Optional[Path] = None,
) -> Path:
    import yaml

    base = {}
    meta = {
        "test": False,
        "data_dir": "/maps/rt582/data",
        "max_workers": 32,
        "delete_original": False,
        "verbose": False,
        "find_alternatives": True,
        "historic_start_year": 2000,
        "historic_end_year": 2014,
        "future_start_year": 2090,
        "future_end_year": 2100,
    }
    if base_yaml and base_yaml.exists():
        loaded = fileops.read_yaml(base_yaml)
        base = loaded.get("search_criteria", {})
        meta = loaded.get("meta_criteria", meta)

    payload = {
        "search_criteria": build_supplement_search_criteria(
            variables=("ph", "talk"),
            experiments=("historical",),
            top_n=25,
            base_criteria=base,
        ),
        "meta_criteria": meta,
    }
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w") as handle:
        yaml.safe_dump(payload, handle, sort_keys=False, default_flow_style=False)
    return path


def build_panel_search_criteria(
    panel: Iterable[str],
    *,
    base_criteria: Optional[dict] = None,
) -> dict:
    """Build search_criteria for a fixed-model panel download (no per-variable top_n cap)."""
    panel_str = ",".join(panel)
    criteria = dict(base_criteria or {})
    criteria.update(
        {
            "project": "CMIP6",
            "table_id": "Omon",
            "experiment_id": ",".join(EXPERIMENTS),
            "variable": ",".join(VARIABLES),
            "grid_label": criteria.get("grid_label", "gn,gr"),
            "member_id": criteria.get("member_id", "r1i1p1f1"),
            "source_id": panel_str,
        }
    )
    filt = dict(criteria.get("filter") or {})
    filt.pop("top_n", None)
    filt.setdefault("limit", 5000)
    criteria["filter"] = filt
    return criteria


def build_gap_search_jobs(gaps: Iterable[EnsembleGap]) -> list[dict]:
    """One tight ESGF search job per missing (variable, experiment, model)."""
    jobs = []
    seen = set()
    for gap in gaps:
        if gap.status != "missing_raw":
            continue
        if gap.key in seen:
            continue
        seen.add(gap.key)
        institution_id = "NOAA-GFDL" if gap.model.startswith("GFDL") else None
        job = {
            "project": "CMIP6",
            "table_id": "Omon",
            "variable": gap.variable,
            "experiment_id": gap.experiment,
            "source_id": gap.model,
            "activity_id": activity_id_for_experiment(gap.experiment),
            "member_id": "r1i1p1f1",
            "grid_label": "gn,gr",
            "filter": {"limit": 200},
        }
        if institution_id:
            job["institution_id"] = institution_id
        jobs.append(job)
    return jobs


def write_panel_search_yaml(
    path: Path,
    panel: Iterable[str],
    *,
    base_yaml: Optional[Path] = None,
) -> Path:
    """Write search_ensemble_panel.yaml for panel-wide downloads."""
    import yaml

    base = {}
    meta = {
        "test": False,
        "data_dir": "/maps/rt582/data",
        "max_workers": 32,
        "delete_original": False,
        "verbose": False,
        "find_alternatives": True,
        "historic_start_year": 2000,
        "historic_end_year": 2014,
        "future_start_year": 2090,
        "future_end_year": 2100,
    }
    if base_yaml and base_yaml.exists():
        loaded = fileops.read_yaml(base_yaml)
        base = loaded.get("search_criteria", {})
        meta = loaded.get("meta_criteria", meta)

    payload = {
        "search_criteria": build_panel_search_criteria(panel, base_criteria=base),
        "meta_criteria": meta,
    }
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w") as handle:
        yaml.safe_dump(payload, handle, sort_keys=False, default_flow_style=False)
    return path


def activity_id_for_experiment(experiment: str) -> str:
    return "CMIP" if experiment == "historical" else "ScenarioMIP"


def raw_chunks_needing_regrid(
    raw_files: Iterable[Path],
    regrid_files: Iterable[Path],
    y0: int,
    y1: int,
) -> list[Path]:
    """Return raw files overlapping the target period with no matching regridded chunk."""
    regrid_names = [path.name for path in regrid_files]
    missing: list[Path] = []
    for path in raw_files:
        if not file_period_overlaps(path, y0, y1):
            continue
        match = FILE_PERIOD_RE.search(path.name)
        if not match:
            continue
        period_token = match.group(0)
        if not any(period_token in name for name in regrid_names):
            missing.append(path)
    return sorted(missing)


def analyze_processing_gaps(
    *,
    data_dir: Path,
    regridded_dir: Path,
    panel: Iterable[str] = DEFAULT_PANEL_MODELS,
    target: int = TARGET_MODELS,
    required: str = REQUIRED_MODEL,
) -> pd.DataFrame:
    """Actionable download/regrid gaps for selected panel models."""
    raw_index = index_raw_cmip6_files(data_dir)
    regrid_index = index_regridded_files(regridded_dir)
    rows: list[dict] = []

    for variable in VARIABLES:
        for experiment in EXPERIMENTS:
            y0, y1 = PERIOD_YEARS[experiment]
            selected = select_models_for_slot(
                variable,
                experiment,
                panel,
                raw_index,
                regrid_index,
                target=target,
                required=required,
            )
            for model in selected:
                key = (variable, model, experiment)
                raw_files = raw_index.get(key, [])
                regrid_files = regrid_index.get(key, [])
                raw_any = has_period_coverage(raw_files, y0, y1)
                regrid_any = has_period_coverage(regrid_files, y0, y1)
                raw_full = has_full_period_coverage(raw_files, y0, y1)
                regrid_full = has_full_period_coverage(regrid_files, y0, y1)

                if not raw_any:
                    rows.append(
                        {
                            "variable": variable,
                            "experiment": experiment,
                            "model": model,
                            "period_start": y0,
                            "period_end": y1,
                            "status": "missing_raw",
                            "action": "search_and_download",
                            "raw_files": "",
                            "regrid_files": "",
                            "notes": "No raw files overlap the analysis period",
                        }
                    )
                    continue

                missing_chunks = raw_chunks_needing_regrid(raw_files, regrid_files, y0, y1)
                if missing_chunks:
                    rows.append(
                        {
                            "variable": variable,
                            "experiment": experiment,
                            "model": model,
                            "period_start": y0,
                            "period_end": y1,
                            "status": "partial_regrid" if regrid_any else "missing_regrid",
                            "action": "regrid",
                            "raw_files": "; ".join(str(p) for p in missing_chunks),
                            "regrid_files": "; ".join(str(p) for p in regrid_files),
                            "notes": (
                                f"{len(missing_chunks)} raw chunk(s) need regridding "
                                f"for full {y0}-{y1} coverage"
                            ),
                        }
                    )
                elif not regrid_full and regrid_any:
                    rows.append(
                        {
                            "variable": variable,
                            "experiment": experiment,
                            "model": model,
                            "period_start": y0,
                            "period_end": y1,
                            "status": "partial_regrid",
                            "action": "regrid",
                            "raw_files": "; ".join(str(p) for p in raw_files),
                            "regrid_files": "; ".join(str(p) for p in regrid_files),
                            "notes": (
                                f"Regridded files exist but do not fully cover {y0}-{y1}"
                            ),
                        }
                    )
                elif raw_full and not regrid_any:
                    rows.append(
                        {
                            "variable": variable,
                            "experiment": experiment,
                            "model": model,
                            "period_start": y0,
                            "period_end": y1,
                            "status": "missing_regrid",
                            "action": "regrid",
                            "raw_files": "; ".join(str(p) for p in raw_files),
                            "regrid_files": "",
                            "notes": "Raw coverage exists; no regridded output yet",
                        }
                    )

            if required not in selected:
                key = (variable, required, experiment)
                raw_files = raw_index.get(key, [])
                if not has_period_coverage(raw_files, y0, y1):
                    rows.append(
                        {
                            "variable": variable,
                            "experiment": experiment,
                            "model": required,
                            "period_start": y0,
                            "period_end": y1,
                            "status": "missing_raw",
                            "action": "search_and_download",
                            "raw_files": "",
                            "regrid_files": "; ".join(
                                str(p) for p in regrid_index.get(key, [])
                            ),
                            "notes": (
                                f"Required model {required} has no raw data for this slot"
                            ),
                        }
                    )

    return pd.DataFrame(rows)


def write_processing_gaps_csv(
    processing_gaps: pd.DataFrame,
    path: Path,
) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    processing_gaps.to_csv(path, index=False)
    return path


def write_gap_search_yamls(
    gaps: Iterable[EnsembleGap],
    out_dir: Path,
    *,
    base_yaml: Optional[Path] = None,
) -> list[Path]:
    """Write one targeted search YAML per missing-raw panel gap."""
    import yaml

    base = {}
    meta = {
        "test": False,
        "data_dir": "/maps/rt582/data",
        "max_workers": 32,
        "delete_original": False,
        "verbose": False,
        "find_alternatives": True,
        "historic_start_year": 2000,
        "historic_end_year": 2014,
        "future_start_year": 2090,
        "future_end_year": 2100,
    }
    if base_yaml and base_yaml.exists():
        loaded = fileops.read_yaml(base_yaml)
        base = loaded.get("search_criteria", {})
        meta = loaded.get("meta_criteria", meta)

    out_dir.mkdir(parents=True, exist_ok=True)
    written: list[Path] = []
    seen: set[tuple[str, str, str]] = set()
    for gap in gaps:
        if gap.status != "missing_raw" or gap.key in seen:
            continue
        seen.add(gap.key)
        criteria = dict(base)
        criteria.update(
            {
                "project": "CMIP6",
                "table_id": "Omon",
                "variable": gap.variable,
                "experiment_id": gap.experiment,
                "source_id": gap.model,
                "institution_id": "NOAA-GFDL" if gap.model.startswith("GFDL") else criteria.get("institution_id"),
                "activity_id": activity_id_for_experiment(gap.experiment),
                "member_id": "r1i1p1f1",
                "grid_label": criteria.get("grid_label", "gn,gr"),
                "filter": {"limit": 500},
            }
        )
        criteria.pop("filter", None)
        criteria["filter"] = {"limit": 500}
        path = out_dir / f"search_gap_{gap.variable}_{gap.experiment}_{gap.model}.yaml"
        payload = {
            "search_criteria": criteria,
            "meta_criteria": meta,
            "_note": (
                f"Targeted download for {gap.variable}/{gap.experiment}/{gap.model}. "
                "If search returns 0 files, delete the matching empty cache file under "
                "data/search_results/ and retry, or download manually from the publishing node "
                "(often esgdata.gfdl.noaa.gov or esgf-node.ornl.gov)."
            ),
        }
        with path.open("w") as handle:
            yaml.safe_dump(payload, handle, sort_keys=False, default_flow_style=False)
        written.append(path)
    return written


def write_gap_jobs_csv(gaps: Iterable[EnsembleGap], path: Path) -> Path:
    rows = [
        {
            "variable": g.variable,
            "experiment": g.experiment,
            "model": g.model,
            "period_start": g.period[0],
            "period_end": g.period[1],
            "status": g.status,
        }
        for g in gaps
    ]
    df = pd.DataFrame(rows)
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(path, index=False)
    return path


def coverage_pivot(summary: pd.DataFrame) -> pd.DataFrame:
    """Compact view: model counts per variable/experiment."""
    sub = summary[summary["model"] == "__summary__"].copy()
    return sub.set_index(["variable", "experiment"])[
        ["n_models_selected", "n_models_available", "has_required", "period"]
    ]
