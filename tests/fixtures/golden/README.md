# Golden Fixtures

Put stable reference cases here when the expected output should remain intentionally fixed.

Golden fixtures are useful for:

- locked glossary behavior
- stable relation memory decisions
- known-safe subtitle/TTS pairs
- export/QC invariants
- semantic cases with a fixed expected QC outcome

Do not use golden fixtures as a dumping ground for unresolved ambiguous cases.

## Golden semantic dataset

- Register reusable semantic reference cases in `semantic_dataset_manifest.json`.
- Register reusable review-routing reference cases in `review_gate_dataset_manifest.json`.
- Semantic QC cases should declare:
  - `source_run`
  - `class`
  - `expected_outcome`
  - `expected.error_count`
  - `expected.warning_count`
  - required/forbidden QC codes
- Review-gate cases should declare:
  - `source_run`
  - `class`
  - `expected_outcome`
- Use this for cases where we want refactors to preserve behavior exactly, whether the intended outcome is:
  - safe pass
  - fail-safe review/error
