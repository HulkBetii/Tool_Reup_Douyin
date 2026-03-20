# Fixtures

Use this tree for committed non-binary fixtures that support regression-oriented testing.

## Layout

- `regression/`
  - minimal failing fixtures extracted from real bugs
- `golden/`
  - stable reference fixtures and semantic dataset cases that should keep their expected outcome across refactors

## Rules

- Prefer JSON/YAML/text over large binary files.
- Keep each fixture minimal but semantically meaningful.
- Register new fixtures in `manifest.json`.
- If the source sample is sensitive, anonymize it.
- If a semantic case has a stable intended outcome, add it to `golden/semantic_dataset_manifest.json` so the dataset harness runs it automatically.
- If the stable contract is about review routing rather than QC counters, add it to `golden/review_gate_dataset_manifest.json`.
