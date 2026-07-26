#!/usr/bin/env python3
"""Backward-compatible wrapper; prefer ``esgplus search-analysis``."""

from esgpull.esgpullplus.search_analysis_cli import run_search_analysis


def main() -> None:
    run_search_analysis()


if __name__ == "__main__":
    main()
