To be populated with changes between versions.

## Version 1.1.0

- Add unified **`esgplus`** CLI: `download`, `search`, `search-analysis`.
- Make **`cdo-toolkit`** optional via the `[processing]` extra (search/download work without it).
- Apply `filter.limit` after time-subsetting in search (year filters in `meta_criteria`).
- Lazy matplotlib import for search analysis; `[plotting]` optional extra unchanged.

## Version 1.0.0

Ironed out a lot of the kinks, replacing them with a (currently) seamless download, subsetting, and regridding experience.
