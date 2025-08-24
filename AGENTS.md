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
- **Location**: `tests/` mirrors `src/`; name files `test_*.py` and test functions `test_*`.
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

