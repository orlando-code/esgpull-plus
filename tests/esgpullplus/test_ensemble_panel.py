from pathlib import Path

from esgpull.esgpullplus.ensemble_panel import (
    DEFAULT_PANEL_MODELS,
    REQUIRED_MODEL,
    build_gap_search_jobs,
    build_panel_search_criteria,
    select_models_for_slot,
)


def test_build_panel_search_criteria_drops_top_n():
    criteria = build_panel_search_criteria(
        DEFAULT_PANEL_MODELS,
        base_criteria={"filter": {"top_n": 10, "limit": 1000}},
    )
    assert REQUIRED_MODEL in criteria["source_id"]
    assert "top_n" not in criteria["filter"]
    assert criteria["variable"] == "tos,ph,talk"


def test_select_models_for_slot_prefers_required_and_panel(tmp_path):
    raw = tmp_path / "CMIP6"
    paths = {}
    for model in (REQUIRED_MODEL, "GFDL-CM4"):
        path = (
            raw
            / "CMIP"
            / "NOAA-GFDL"
            / model
            / "historical"
            / "r1i1p1f1"
            / "Omon"
            / "tos"
            / "gn"
            / "v20200701"
            / f"tos_Omon_{model}_historical_r1i1p1f1_gn_200001-201412.nc"
        )
        path.parent.mkdir(parents=True, exist_ok=True)
        path.touch()
        paths[("tos", model, "historical")] = [path]

    chosen = select_models_for_slot(
        "tos",
        "historical",
        DEFAULT_PANEL_MODELS,
        paths,
        {},
        target=5,
        required=REQUIRED_MODEL,
    )
    assert chosen[0] == REQUIRED_MODEL
    assert len(chosen) <= 5


def test_build_gap_search_jobs_only_missing_raw():
    from esgpull.esgpullplus.ensemble_panel import EnsembleGap

    gaps = [
        EnsembleGap("ph", "historical", "GFDL-CM4", (2005, 2014), "missing_raw"),
        EnsembleGap("ph", "historical", "GFDL-CM4", (2005, 2014), "missing_regridded"),
    ]
    jobs = build_gap_search_jobs(gaps)
    assert len(jobs) == 1
    assert jobs[0]["source_id"] == "GFDL-CM4"
