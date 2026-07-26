"""Optional ``cdo-toolkit`` integration for regridding and post-processing."""

from __future__ import annotations

from typing import Any, Callable

_PROCESSING_INSTALL_HINT = (
    'Install processing extras: pip install "esgpull-plus[processing]" '
    "(also requires the CDO binary, e.g. conda install -c conda-forge cdo)"
)


def import_cdo_toolkit() -> Any:
    """Return the ``cdo_toolkit`` module, or raise a clear ImportError."""
    try:
        import cdo_toolkit
    except ImportError as exc:
        raise ImportError(
            f"cdo-toolkit is required for regridding. {_PROCESSING_INSTALL_HINT}"
        ) from exc
    return cdo_toolkit


def get_regrid_directory() -> Callable[..., Any]:
    return import_cdo_toolkit().regrid_directory


def get_process_single_file_standalone() -> Callable[..., Any]:
    return import_cdo_toolkit().process_single_file_standalone
