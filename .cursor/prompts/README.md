# Prompts

Reusable prompt snippets/templates for Cursor. Use these to guide common tasks consistently.

Suggested templates to add over time:
- add-test-coverage.md: Checklist and steps to increase coverage for a module
- refactor-module.md: Safe refactor checklist and acceptance criteria
- add-dataset-loader.md: Template for adding a new ingestion pipeline with Polars-lazy
- write-benchmark.md: Setup for quick performance comparisons (lazy vs eager)

Conventions:
- Keep prompts short and action-oriented.
- Reference repo rules in `.cursor/rules.md`.
- Prefer Polars-lazy pipelines and test-first changes where feasible.
