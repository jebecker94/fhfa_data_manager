## Architecture Overview

This document summarizes the repository structure, key modules, data flow, and conventions to guide development and reviews.

### High-Level Structure
- `src/fhfa_data_manager/`: Library code (domain logic, data access, transformations)
- `tests/`: Unit/integration tests mirroring `src`
- `scripts/`: One-off utilities / CLI entry points
- `data/{raw,processed}/`: Local data (ignored by Git)
- `configs/`: Reusable YAML/JSON configuration
- `assets/`: Static files (schemas, sample inputs)
- `.cursor/`: Assistant-facing rules, prompts, and docs

### Technology Choices
- Python ≥ 3.10 with type hints
- Polars (lazy) as the default DataFrame engine
- Pytest for testing; Ruff + Black (or `ruff format`) for lint/format
- `pathlib.Path` for filesystem paths; `logging` for observability

### Data Flow (Conceptual)
1. Ingestion
   - Read from CSV/Parquet or external sources using Polars lazy scans (`pl.scan_*`).
   - Enforce schema and dtypes at read time when possible.
2. Transformation
   - Build lazy pipelines using expressions (select/with_columns/filter/join/group_by).
   - Keep intermediate results lazy; avoid materialization until boundaries.
3. Output
   - Materialize with `.collect()` at I/O boundaries only.
   - Persist artifacts (e.g., Parquet) under `data/processed/`.

### Modules & Responsibilities (indicative)
- Ingestion: interfaces to raw sources; schema validation; lazy scans
- Transformation: reusable expression builders; joins/groupings; feature engineering
- IO/Exports: well-typed writers; small boundary-layer functions
- Config: centralized parameterization (paths, formats, feature flags)

### Polars-Lazy Guidance
- Default to lazy: `pl.scan_csv` / `pl.scan_parquet` → expression pipeline → `.collect()` at the edge.
- Prefer columnar expressions and vectorized operations; avoid row-wise Python loops.
- Document any pandas usage; keep scope minimal and local.

### Error Handling & Logging
- Validate inputs at module boundaries; fail fast with clear exceptions.
- Use module-level loggers; include key identifiers (paths, dataset names) in messages.

### Testing Strategy
- Unit tests for pure transformations (deterministic inputs/outputs).
- Integration tests for end-to-end pipelines using small fixtures under `tests/fixtures/`.
- Aim ≥ 85% coverage on core modules; include error-path tests.

### Configuration
- No secrets in repo; use environment variables and `.env.example`.
- Prefer explicit configuration in `configs/` and pass into modules rather than global state.

### Extensibility Notes
- Keep boundaries clean between ingestion, transformation, and IO.
- Favor small, composable functions with explicit inputs/outputs.
- Document expected schemas at module interfaces.
