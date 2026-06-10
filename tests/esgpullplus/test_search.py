import pandas as pd

from esgpull.esgpullplus.search import (
    SearchResults,
    normalize_search_criteria,
    _parse_facet_values,
)


def test_normalize_variant_id_to_member_id():
    criteria = {"project": "CMIP6", "variant_id": "r7i1p1f1"}
    normalized = normalize_search_criteria(criteria)
    assert normalized["member_id"] == "r7i1p1f1"
    assert "variant_id" not in normalized


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
