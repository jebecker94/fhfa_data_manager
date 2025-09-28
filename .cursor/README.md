# .cursor

This directory holds project-specific configuration and context for the Cursor AI assistant. Keeping these files in-repo helps the assistant follow your conventions and priorities when assisting with edits, explanations, and refactors.

Typical contents:

- rules.md or rules/: High-level repository rules and guidelines (style, testing, CI, security)
- prompts/: Reusable prompt snippets/templates for common tasks
- docs/: Lightweight, assistant-facing docs (architecture notes, data flow, API contracts)
- .cursorignore: Like .gitignore but for AI indexing/context (exclude large/sensitive files)
- settings.json (optional): Project-level assistant settings if you use them

Notes:

- Empty directories are not tracked by Git; this README ensures the folder exists.
- Do not store secrets here. Use environment variables and a repo-root .env.example instead.
