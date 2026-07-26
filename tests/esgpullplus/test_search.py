import pandas as pd
import pytest

from esgpull.esgpullplus.search import (
    SearchResults,
    format_missing_limiting_facets_message,
    missing_limiting_search_facets,
    normalize_search_criteria,
    prepare_esgf_search_criteria,
    validate_limiting_search_criteria,
    _parse_facet_values,
)


def test_normalize_variant_id_to_member_id():
    criteria = {"project": "CMIP6", "variant_id": "r7i1p1f1"}
    normalized = normalize_search_criteria(criteria)
    assert normalized["member_id"] == "r7i1p1f1"
    assert "variant_id" not in normalized


def test_normalize_variant_label_to_member_id():
    criteria = {"project": "CMIP6", "variant_label": "r7i1p1f1"}
    normalized = normalize_search_criteria(criteria)
    assert normalized["member_id"] == "r7i1p1f1"
    assert "variant_label" not in normalized


def test_prepare_esgf_search_criteria_uses_metagrid_facets():
    prepared = prepare_esgf_search_criteria(
        {
            "project": "CMIP6",
            "table_id": "Omon",
            "variable": "talk",
            "experiment_id": "historical",
            "member_id": "r1i1p1f1",
            "grid_label": "gr",
        }
    )
    assert prepared["variable_id"] == "talk"
    assert "variable" not in prepared
    assert prepared["variant_label"] == "r1i1p1f1"
    assert "member_id" not in prepared


def test_apply_facet_filters_member_id_matches_variant_label():
    sr = SearchResults(
        search_criteria={
            "project": "CMIP6",
            "variable_id": "talk",
            "experiment_id": "historical",
            "member_id": "r1i1p1f1",
        },
        meta_criteria={},
        file=True,
    )
    df = pd.DataFrame(
        [
            {
                "source_id": "GFDL-ESM4",
                "variant_label": "r1i1p1f1",
                "member_id": "",
                "filename": "a.nc",
            },
            {
                "source_id": "GFDL-ESM4",
                "variant_label": "r2i1p1f1",
                "member_id": "",
                "filename": "b.nc",
            },
        ]
    )
    filtered = sr._apply_facet_filters(df, sr._generate_subsearches()[0])
    assert list(filtered["filename"]) == ["a.nc"]


def test_load_subsearch_from_cache_ignores_empty_negative_cache(tmp_path):
    sr = SearchResults(
        search_criteria={
            "project": "CMIP6",
            "variable_id": "talk",
            "experiment_id": "historical",
        },
        meta_criteria={"data_dir": str(tmp_path)},
        file=True,
    )
    subsearch = sr._generate_subsearches()[0]
    sr.search_results_dir.mkdir(parents=True, exist_ok=True)
    cache_file = sr.search_results_dir / f"{sr._get_subsearch_cache_key(subsearch)}.csv"
    cache_file.touch()
    assert sr._load_subsearch_from_cache(subsearch) is None


def test_save_subsearch_to_cache_refreshes_stale_negative(tmp_path):
    sr = SearchResults(
        search_criteria={
            "project": "CMIP6",
            "table_id": "Omon",
            "variable_id": "talk",
            "experiment_id": "ssp245",
            "member_id": "r1i1p1f1",
            "grid_label": "gn",
        },
        meta_criteria={"data_dir": str(tmp_path)},
        file=True,
    )
    subsearch = sr._generate_subsearches()[0]
    sr.search_results_dir.mkdir(parents=True, exist_ok=True)
    cache_file = sr.search_results_dir / f"{sr._get_subsearch_cache_key(subsearch)}.csv"
    cache_file.touch()
    assert cache_file.stat().st_size == 0

    results = pd.DataFrame(
        [
            {
                "file_id": "abc",
                "dataset_id": "ds1",
                "filename": "talk_Omon_test.nc",
                "variable_id": "talk",
                "experiment_id": "ssp245",
            }
        ]
    )
    sr._save_subsearch_to_cache(subsearch, results)
    assert cache_file.stat().st_size > 0
    reloaded = pd.read_csv(cache_file)
    assert len(reloaded) == 1
    assert reloaded.iloc[0]["filename"] == "talk_Omon_test.nc"


def test_get_top_n_applies_per_variable():
    sr = SearchResults(
        search_criteria={"project": "CMIP6", "filter": {"top_n": 1}},
        meta_criteria={},
        file=True,
    )
    sr.results_df = pd.DataFrame(
        [
            {
                "dataset_id": "tos_a",
                "variable_id": "tos",
                "experiment_id": "historical",
                "resolution": 25.0,
            },
            {
                "dataset_id": "tos_b",
                "variable_id": "tos",
                "experiment_id": "historical",
                "resolution": 50.0,
            },
            {
                "dataset_id": "talk_a",
                "variable_id": "talk",
                "experiment_id": "historical",
                "resolution": 100.0,
            },
            {
                "dataset_id": "talk_b",
                "variable_id": "talk",
                "experiment_id": "historical",
                "resolution": 200.0,
            },
        ]
    )
    sr.top_n = 1
    top = sr.get_top_n()
    assert set(top["dataset_id"]) == {"tos_a", "talk_a"}


def test_load_subsearch_from_cache_uses_broad_cache_when_narrow_negative(tmp_path):
    sr = SearchResults(
        search_criteria={
            "project": "CMIP6",
            "table_id": "Omon",
            "variable_id": "talk",
            "experiment_id": "ssp245",
            "member_id": "r1i1p1f1",
            "grid_label": "gn",
        },
        meta_criteria={"data_dir": str(tmp_path)},
        file=True,
    )
    subsearch = sr._generate_subsearches()[0]
    sr.search_results_dir.mkdir(parents=True, exist_ok=True)

    narrow = sr.search_results_dir / f"{sr._get_subsearch_cache_key(subsearch)}.csv"
    narrow.touch()

    broad = sr._subsearch_cache_path(
        {
            "project": "CMIP6",
            "table_id": "Omon",
            "variable_id": "talk",
            "experiment_id": "ssp245",
        },
        "file",
    )
    pd.DataFrame(
        [
            {
                "filename": "talk_a.nc",
                "member_id": "r1i1p1f1",
                "grid_label": "gn",
                "variable_id": "talk",
            },
            {
                "filename": "talk_b.nc",
                "member_id": "r2i1p1f1",
                "grid_label": "gn",
                "variable_id": "talk",
            },
        ]
    ).to_csv(broad, index=False)

    loaded = sr._load_subsearch_from_cache(subsearch)
    assert loaded is not None
    assert list(loaded["filename"]) == ["talk_a.nc"]


def test_apply_facet_filters_grid_label_from_dataset_id():
    sr = SearchResults(
        search_criteria={
            "project": "CMIP6",
            "variable_id": "ph",
            "experiment_id": "ssp245",
            "grid_label": "gr",
        },
        meta_criteria={},
        file=True,
    )
    df = pd.DataFrame(
        [
            {
                "dataset_id": "CMIP6.ScenarioMIP.NOAA-GFDL.GFDL-ESM4.ssp245.r1i1p1f1.Omon.ph.gr.v20180701",
            },
            {
                "dataset_id": "CMIP6.ScenarioMIP.NOAA-GFDL.GFDL-ESM4.ssp245.r1i1p1f1.Omon.ph.gn.v20180701",
            },
        ]
    )
    filtered = sr._apply_facet_filters(df, sr._generate_subsearches()[0])
    assert len(filtered) == 1
    assert filtered.iloc[0]["dataset_id"].endswith(".gr.v20180701")


def test_cache_key_includes_restricting_facets():
    sr = SearchResults(
        search_criteria={
            "project": "CMIP6",
            "table_id": "Omon",
            "experiment_id": "historical",
            "variable": "tos",
            "source_id": "MPI-ESM1-2-HR",
            "member_id": "r7i1p1f1",
        },
        meta_criteria={},
        file=True,
    )
    subsearch = sr._generate_subsearches()[0]
    key = sr._get_subsearch_cache_key(subsearch)
    assert "MPI-ESM1-2-HR" in key
    assert "r7i1p1f1" in key


def test_apply_facet_filters_from_broad_cache():
    sr = SearchResults(
        search_criteria={
            "project": "CMIP6",
            "table_id": "Omon",
            "experiment_id": "historical",
            "variable": "tos",
            "source_id": "MPI-ESM1-2-HR",
            "member_id": "r7i1p1f1",
        },
        meta_criteria={},
        file=True,
    )
    broad = pd.DataFrame(
        [
            {
                "source_id": "MPI-ESM1-2-HR",
                "member_id": "r7i1p1f1",
                "filename": "a.nc",
            },
            {
                "source_id": "MPI-ESM1-2-HR",
                "member_id": "r1i1p1f1",
                "filename": "b.nc",
            },
            {
                "source_id": "OTHER",
                "member_id": "r7i1p1f1",
                "filename": "c.nc",
            },
        ]
    )
    subsearch = sr._generate_subsearches()[0]
    filtered = sr._apply_facet_filters(broad, subsearch)
    assert list(filtered["filename"]) == ["a.nc"]


def test_parse_facet_values_splits_commas():
    assert _parse_facet_values("a,b") == ["a", "b"]


def test_missing_limiting_search_facets_detects_variable_and_experiment():
    criteria = {"project": "CMIP6", "table_id": "Omon"}
    assert missing_limiting_search_facets(criteria) == ["variable", "experiment_id"]


def test_missing_limiting_search_facets_accepts_variable_id_alias():
    criteria = {
        "project": "CMIP6",
        "variable_id": "tos",
        "experiment_id": "historical",
    }
    assert missing_limiting_search_facets(criteria) == []


def test_validate_limiting_search_criteria_raises_with_clear_message():
    with pytest.raises(ValueError, match="Missing required search criteria: variable"):
        validate_limiting_search_criteria(
            {"project": "CMIP6", "experiment_id": "historical"}
        )


def test_apply_year_filter_df_excludes_out_of_range_files():
    sr = SearchResults(
        search_criteria={"project": "CMIP6", "variable": "tos"},
        meta_criteria={"historic_start_year": 2010, "historic_end_year": 2014},
        file=True,
    )
    df = pd.DataFrame(
        [
            {
                "filename": "tos_Omon_model_historical_r1i1p1f1_gn_201001-201412.nc",
                "experiment_id": "historical",
            },
            {
                "filename": "tos_Omon_model_historical_r1i1p1f1_gn_200901-201112.nc",
                "experiment_id": "historical",
            },
            {
                "filename": "tos_Omon_model_historical_r1i1p1f1_gn_185001-185512.nc",
                "experiment_id": "historical",
            },
        ]
    )
    filtered = sr._apply_year_filter_df(df)
    assert list(filtered["filename"]) == [
        "tos_Omon_model_historical_r1i1p1f1_gn_201001-201412.nc",
        "tos_Omon_model_historical_r1i1p1f1_gn_200901-201112.nc",
    ]


def test_finalize_search_results_applies_time_filter_before_limit():
    sr = SearchResults(
        search_criteria={
            "project": "CMIP6",
            "variable": "tos",
            "filter": {"limit": 1},
        },
        meta_criteria={"historic_start_year": 2010, "historic_end_year": 2014},
        file=True,
    )
    sr.results_df = pd.DataFrame(
        [
            {
                "filename": "tos_Omon_model_historical_r1i1p1f1_gn_185001-185512.nc",
                "experiment_id": "historical",
                "dataset_id": "d1",
                "nominal_resolution": "100 km",
            },
            {
                "filename": "tos_Omon_model_historical_r1i1p1f1_gn_201001-201412.nc",
                "experiment_id": "historical",
                "dataset_id": "d2",
                "nominal_resolution": "100 km",
            },
            {
                "filename": "tos_Omon_model_historical_r1i1p1f1_gn_200901-201112.nc",
                "experiment_id": "historical",
                "dataset_id": "d3",
                "nominal_resolution": "100 km",
            },
        ]
    )
    files = sr._finalize_search_results()
    assert len(files) == 1
    assert "201001-201412" in files[0].filename


def test_format_missing_limiting_facets_message():
    msg = format_missing_limiting_facets_message(["variable"])
    assert "variable" in msg
    assert "search.yaml" in msg
