# AGENTS.md

This repository is a local Windows desktop app for the pipeline:

`video (zh) -> ASR -> semantic translation -> subtitle_text -> tts_text -> TTS -> export`

## Primary engineering rule

Do not patch only the visible sample output.

Every real bug should become permanent codebase knowledge through:

1. minimal failing fixture when practical
2. failing regression test before the fix
3. root-cause analysis at the correct layer
4. smallest general fix that addresses the class of bug
5. regression guard (QC rule, gate, invariant, or review routing)
6. blast-radius audit
7. spec/doc updates when behavior or contracts change

If a case is still ambiguous, prefer:

- `needs_human_review = true`
- semantic QC issue / fail
- blocked TTS/export

Do not guess and silently ship user-facing output.

## Bugfix workflow for this repo

Use the workflow in:

- [docs/AI_BUGFIX_WORKFLOW.md](C:\Users\HulkBeoti\Documents\Reup_Video\docs\AI_BUGFIX_WORKFLOW.md)

## Core semantic invariants

- `subtitle_text` and `tts_text` must not drift in honorific policy without explicit reason.
- If speaker/listener/relation evidence is weak, the system should stay conservative or route to review.
- TTS/export must not proceed through semantic cases that are not safe enough.
- Fixes must target the bug class, not one literal sentence unless it is a deliberate glossary entry.

## Where to add permanent knowledge

- Codebase map: [docs/CODEBASE_MAP.md](C:\Users\HulkBeoti\Documents\Reup_Video\docs\CODEBASE_MAP.md)
- Error taxonomy: [docs/ERROR_TAXONOMY.md](C:\Users\HulkBeoti\Documents\Reup_Video\docs\ERROR_TAXONOMY.md)
- Semantic QC rules/spec: [docs/SEMANTIC_QC_SPEC.md](C:\Users\HulkBeoti\Documents\Reup_Video\docs\SEMANTIC_QC_SPEC.md)
- Review reason codes: [docs/REVIEW_REASON_CODES.md](C:\Users\HulkBeoti\Documents\Reup_Video\docs\REVIEW_REASON_CODES.md)
- Known limitations: [docs/KNOWN_LIMITATIONS.md](C:\Users\HulkBeoti\Documents\Reup_Video\docs\KNOWN_LIMITATIONS.md)

## Fixture and test locations

- regression fixtures: [tests/fixtures/regression](C:\Users\HulkBeoti\Documents\Reup_Video\tests\fixtures\regression)
- golden fixtures: [tests/fixtures/golden](C:\Users\HulkBeoti\Documents\Reup_Video\tests\fixtures\golden)
- fixture manifest: [tests/fixtures/manifest.json](C:\Users\HulkBeoti\Documents\Reup_Video\tests\fixtures\manifest.json)
- semantic QC tests: [tests/semantic_qc](C:\Users\HulkBeoti\Documents\Reup_Video\tests\semantic_qc)
- integration tests: [tests/integration](C:\Users\HulkBeoti\Documents\Reup_Video\tests\integration)

## Important implementation files

- app entry: [main.py](C:\Users\HulkBeoti\Documents\Reup_Video\main.py), [src/app/main.py](C:\Users\HulkBeoti\Documents\Reup_Video\src\app\main.py)
- project schema/state: [src/app/project/database.py](C:\Users\HulkBeoti\Documents\Reup_Video\src\app\project\database.py)
- contextual pipeline: [src/app/translate/contextual_pipeline.py](C:\Users\HulkBeoti\Documents\Reup_Video\src\app\translate\contextual_pipeline.py)
- semantic QC: [src/app/translate/semantic_qc.py](C:\Users\HulkBeoti\Documents\Reup_Video\src\app\translate\semantic_qc.py)
- prompt presets: [src/app/translate/presets.py](C:\Users\HulkBeoti\Documents\Reup_Video\src\app\translate\presets.py)
- UI gates/review: [src/app/ui/main_window.py](C:\Users\HulkBeoti\Documents\Reup_Video\src\app\ui\main_window.py)
