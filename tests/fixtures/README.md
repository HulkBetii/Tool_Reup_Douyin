# Fixtures

Use this tree for committed non-binary fixtures that support regression-oriented testing.

## Layout

- `regression/`
  - minimal failing fixtures extracted from real bugs
- `golden/`
  - stable reference fixtures that should keep passing across refactors

## Rules

- Prefer JSON/YAML/text over large binary files.
- Keep each fixture minimal but semantically meaningful.
- Register new fixtures in `manifest.json`.
- If the source sample is sensitive, anonymize it.

