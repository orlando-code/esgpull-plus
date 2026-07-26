"""Tests for optional cdo-toolkit integration."""

import pytest

from esgpull.esgpullplus import cdo_optional


def test_import_cdo_toolkit_raises_without_processing_extra(monkeypatch):
    import builtins

    real_import = builtins.__import__

    def fake_import(name, *args, **kwargs):
        if name == "cdo_toolkit":
            raise ImportError("No module named 'cdo_toolkit'")
        return real_import(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", fake_import)
    with pytest.raises(ImportError, match="esgpull-plus\\[processing\\]"):
        cdo_optional.import_cdo_toolkit()
