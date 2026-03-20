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

- dataset manifest:
  - [tests/fixtures/golden/semantic_dataset_manifest.json](C:\Users\HulkBeoti\Documents\Reup_Video\tests\fixtures\golden\semantic_dataset_manifest.json)
- fixture folder:
  - [tests/fixtures/golden](C:\Users\HulkBeoti\Documents\Reup_Video\tests\fixtures\golden)
- harness:
  - [tests/semantic_qc/test_golden_semantic_dataset.py](C:\Users\HulkBeoti\Documents\Reup_Video\tests\semantic_qc\test_golden_semantic_dataset.py)

## Contract

Each dataset entry should declare:

- `fixture_id`
- `path`
- `expected.error_count`
- `expected.warning_count`
- `expected.required_codes`
- `expected.forbidden_codes`

Each fixture should stay minimal, anonymized if needed, and semantically meaningful.

## When to add a case

Add a golden case when:

- the intended semantic behavior is stable
- we want future refactors to preserve it exactly
- the case is no longer ambiguous after review/policy decisions

Do not add unresolved ambiguous cases here. Those belong in regression fixtures or review workflows until the policy is clear.

## Current scope

Initial dataset covers:

- stable family-style honorific consistency
- neutral narration
- locked relation allowed alternates
- side-specific alternate contract
- reviewed real-world dialogue from `Video_test_TQ`
- real-world fail-safe cases for pronoun divergence and low-confidence single-turn ambiguity
