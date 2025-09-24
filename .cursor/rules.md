# Repository Guidelines

## Project Structure & Module Organization
- `src/fhfa_data_manager/`: Library code and modules.
- `tests/`: Unit and integration tests (mirrors `src` layout).
- `scripts/`: One-off or CLI utilities for data tasks.
- `data/{raw,processed}/`: Local data (ignored by Git). Do not commit.
- `notebooks/`: Exploratory analysis and prototypes.
- `configs/`: Reusable YAML/JSON configuration files.
- `assets/`: Static files (schemas, sample inputs).

## Build, Test, and Development Commands
- `python -m venv .venv && . .venv/bin/activate` (Windows: `.venv\Scripts\activate`): Create/activate venv.
- `pip install -e .[dev]`: Install package + dev tools.
- `pytest -q`: Run test suite.
- `pytest --cov=fhfa_data_manager`: Run tests with coverage.
- `ruff check . && ruff format .` (or `black .`): Lint and format.
- `python -m fhfa_data_manager ...`: Run package entry points if provided.

## Coding Style & Naming Conventions
- **Indentation**: 4 spaces; target Python 3.10+.
- **Style**: PEP 8 + type hints; prefer `dataclasses` and `pathlib`.
- **Naming**: `snake_case` (functions/vars), `PascalCase` (classes), `UPPER_SNAKE` (constants).
- **Imports**: standard → third-party → local; avoid wildcard imports.
- **Tools**: Ruff for linting; Black (or Ruff format) for formatting.

## Testing Guidelines
- **Framework**: `pytest`.
- **Location**: `tests/` mirrors `src`; name files `test_*.py` and test functions `test_*`.
- **Fixtures** in `tests/conftest.py`.
- **Coverage**: aim ≥ 85% on core modules; exercise error paths.
- **Run**: `pytest -q`; with coverage: `pytest --cov=fhfa_data_manager`.

## Commit & Pull Request Guidelines
- **Commits**: Conventional Commits (e.g., `feat:`, `fix:`, `docs:`, `refactor:`, `test:`, `chore:`), present tense, concise subject line.
- **PRs**: Clear description, link issues (e.g., `Closes #123`), list changes, include screenshots or sample CLI output if relevant.
- **Checks**: Lint, format, and tests must pass; add/adjust tests for new behavior.
- **Scope**: Prefer small, focused PRs.

## Security & Configuration Tips
- Never commit secrets; use `.env` (and provide `.env.example`).
- Parameterize file paths and credentials via environment/configs.
- Large data/artifacts: use Git LFS or external storage; keep `data/` in `.gitignore`.

---

## DataFrames: Polars-First with Lazy Evaluation
- Prefer Polars over pandas; use lazy APIs by default.
- Ingest with lazy scans (e.g., `pl.scan_csv`, `pl.scan_parquet`).
- Compose transformations with expressions; only `collect()` at I/O or API boundaries.
- Avoid materializing intermediate DataFrames; keep pipelines lazy.
- Prefer schema-safe operations; set dtypes on read when possible.
- For joins/group-bys, ensure keys are well-typed and documented.
- Only use pandas when a capability is missing in Polars; document the reason and keep the scope minimal.

Example pattern:

```python
import polars as pl
from pathlib import Path

raw_dir = Path("data/raw")
scan = pl.scan_csv(raw_dir / "input.csv", dtypes={"id": pl.Int64, "ts": pl.Datetime})

result = (
    scan
    .with_columns([
        (pl.col("value_a") + pl.col("value_b")).alias("value_total"),
        pl.col("ts").dt.truncate("1d").alias("day")
    ])
    .group_by(["id", "day"])  # lazy group-by
    .agg(pl.sum("value_total").alias("sum_value"))
)

# Materialize at the boundary only
output_df = result.collect()
output_df.write_parquet("data/processed/summary.parquet")
```

## I/O and Paths
- Use `pathlib.Path` and path-join operators instead of string concatenation.
- Parameterize file paths via configs or environment variables; avoid hard-coding.
- Keep I/O at boundaries; transformations should be pure where feasible.

## Logging and Errors
- Use `logging` (module-level logger) instead of `print` in library code.
- Fail fast with informative exceptions; validate inputs at boundaries.

## Notebooks
- Keep heavy exploration in `notebooks/`; commit only small, reviewed notebooks.
- Large outputs, caches, and checkpoints should be ignored.

## Assistant-Specific Guidance
- Apply the shared rules above consistently (Cursor, Codex, and other agents).
- Prefer Polars-lazy pipelines; avoid pandas unless required and justified.
- When editing code, add or propose minimal tests under `tests/` for changed behavior.
- Avoid breaking API changes; if necessary, document migration steps clearly.
- Do not include or generate large data files in the repository.
