# FHFA Data Manager

Streamlined tools for downloading, validating, and transforming FHFA-related datasets.

## Quick Start

1) Create a virtual environment

- Windows (PowerShell)
  - `python -m venv .venv`
  - `.venv\\Scripts\\Activate.ps1`
- macOS/Linux
  - `python -m venv .venv`
  - `. .venv/bin/activate`

2) Install dependencies

- `pip install -e .[dev]`

3) Configure environment

- Copy `.env.example` to `.env` and adjust paths/keys as needed.
- Defaults place data under `./data/{raw,processed}`.

4) Run tests

- `pytest -q`
- With coverage: `pytest --cov=fhfa_data_manager`

## Developer Guide

- See `AGENTS.md` for project structure, commands, coding style, testing, and PR conventions.

## Data Loader API

The `fhfa_data_manager` package provides reusable classes for FHFA and FHLB ETL tasks
so notebooks and scripts can share the same orchestration logic.

### Configuration helpers

- `PathsConfig` centralizes directory discovery using environment variables
  (`PROJECT_DIR`, `DATA_DIR`, `FHFA_RAW_DIR`, `FHFA_CLEAN_DIR`). Call
  `PathsConfig.from_env()` to materialize a configuration with sensible
  defaults when the variables are absent.
- `ImportOptions` exposes operational flags (overwrite behaviour, download
  pauses, Excel engine) that can be passed into each loader for customization.

### FHFADataLoader

`FHFADataLoader` gathers FHFA-specific helpers:

- Resolve dictionary files for raw text releases via
  `resolve_dictionary_for_data_file` / `resolve_dictionaries_for_year_folder`.
- Download the latest ZIPs and dictionary PDFs, extract tables, and persist the
  parsed results as CSV and/or Parquet.
- Load fixed-width text files into Polars DataFrames with schema/width metadata
  sourced from the parsed dictionaries, then optionally convert entire year
  folders into compressed Parquet datasets.

### FHLBDataLoader

`FHLBDataLoader` mirrors the FHFA loader for Federal Home Loan Bank artefacts:

- Download CSV releases from the FHFA portal with substring filtering.
- Combine quarterly member spreadsheets across multiple years with consistent
  field naming and date normalization.
- Import and clean acquisitions data using the shared dictionary mappings, and
  emit per-year as well as combined outputs.

### Example

```python
from pathlib import Path

from fhfa_data_manager import PathsConfig, FHFADataLoader

paths = PathsConfig.from_env()
fhfa_loader = FHFADataLoader(paths)

mapping = fhfa_loader.resolve_dictionaries_for_year_folder(2023)
for data_path, dict_path in mapping.items():
    if dict_path is None:
        continue
    table = fhfa_loader.load_fixed_width_dataset(data_path, dict_path)
    table.write_parquet(Path(paths.clean_dir) / f"{data_path.stem}.parquet")
```

## Notes

- Do not commit data or secrets. `data/` and `.env*` are ignored by default (see `.gitignore`).
- Prefer `ruff` for linting and formatting: `ruff check . && ruff format .`.
