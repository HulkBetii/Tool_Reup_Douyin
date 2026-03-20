# Golden Semantic Dataset

This dataset is the stable reference layer for semantic behavior that should remain intentional across refactors.

## Purpose

Use it when a zh->vi semantic case has a known intended outcome and we want the repo to keep enforcing that outcome automatically.

Typical examples:

- a dialogue pair that should pass cleanly
- a narration line that should stay neutral
- a locked relation case that must block when directionality drifts
- an allowed alternate case that must stay safe

## Storage

- semantic QC dataset manifest:
  - [tests/fixtures/golden/semantic_dataset_manifest.json](C:\Users\HulkBeoti\Documents\Reup_Video\tests\fixtures\golden\semantic_dataset_manifest.json)
- review gate dataset manifest:
  - [tests/fixtures/golden/review_gate_dataset_manifest.json](C:\Users\HulkBeoti\Documents\Reup_Video\tests\fixtures\golden\review_gate_dataset_manifest.json)
- fixture folder:
  - [tests/fixtures/golden](C:\Users\HulkBeoti\Documents\Reup_Video\tests\fixtures\golden)
- semantic QC harness:
  - [tests/semantic_qc/test_golden_semantic_dataset.py](C:\Users\HulkBeoti\Documents\Reup_Video\tests\semantic_qc\test_golden_semantic_dataset.py)
- review gate harness:
  - [tests/integration/test_review_gate_dataset.py](C:\Users\HulkBeoti\Documents\Reup_Video\tests\integration\test_review_gate_dataset.py)

## Contract

Each dataset entry should declare:

- `fixture_id`
- `path`
- `source_run`
- `class`
- `expected_outcome`
- `expected.error_count`
- `expected.warning_count`
- `expected.required_codes`
- `expected.forbidden_codes`

For review-gate entries, use:

- `fixture_id`
- `path`
- `source_run`
- `class`
- `expected_outcome`

Each fixture should stay minimal, anonymized if needed, and semantically meaningful.

## When to add a case

Add a golden case when:

- the intended semantic behavior is stable
- we want future refactors to preserve it exactly
- the case is no longer ambiguous after review/policy decisions

Do not add unresolved ambiguous cases to the semantic QC manifest. Those belong in regression fixtures or the review-gate manifest until the routing policy is clear.

## Current scope

Current dataset covers:

- stable family-style honorific consistency
- neutral narration
- locked relation allowed alternates
- side-specific alternate contract
- reviewed real-world dialogue from `Video_test_TQ`
- real-world fail-safe cases for pronoun divergence and low-confidence single-turn ambiguity
- reviewed object-reference cases from `Shinchan`
- reviewed technical narration/object phrase cases from `Wilderness`
- pre-review fail-safe routing for:
  - `ambiguous_term`
  - `ambiguous_object_reference`
  - `uncertain_speaker`
  - `unclear_relationship`
  - `tone_ambiguity`
  - `insufficient_context`
