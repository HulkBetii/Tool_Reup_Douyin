# Review Reason Codes

This file records stable review reason codes used by contextual translation and semantic QC.

## Purpose

Review reason codes should be:

- short
- normalized
- stable across prompts, runtime normalization, tests, and fixtures

This keeps real-world regressions assertable.

## Current normalization hook

- [src/app/translate/contextual_runtime.py](C:\Users\HulkBeoti\Documents\Reup_Video\src\app\translate\contextual_runtime.py)

## Current code families

- `uncertain_speaker`
- `uncertain_listener`
- `ambiguous_term`
- `technical_term_uncertainty`
- `ambiguous_reference`
- `ambiguous_object_reference`
- `ambiguous_damage_description`
- `tone_ambiguity`
- `unspecified_review_reason`

## Semantic QC related codes

These may come from deterministic QC instead of the model:

- `low_confidence_gate`
- `addressee_mismatch`
- `pronoun_without_evidence`
- `sub_tts_pronoun_divergence`
- `honorific_drift`
- `directionality_mismatch`

## Severity note

- `sub_tts_pronoun_divergence` is not always equally severe.
- When `tts_text` is the only side injecting pronoun/vocative and listener or ellipsis evidence is weak, it should be treated as blocking semantic QC, not just a cosmetic warning.
- `directionality_mismatch` should also be treated as blocking when it violates a locked/confirmed relationship memory, because that means the reviewer has already frozen the intended honorific direction.

## Rules

- Prefer normalized snake_case codes over raw model prose.
- If model output is noisy, normalize before persisting and testing.
- `technical_term_uncertainty` is reserved for narration/term-sheet style cases where a term or named entity is central enough that the runtime should hold the segment in review instead of silently guessing a Vietnamese rendering.
- `ambiguous_term` can still be cleared automatically in narration fast v2 for narrow deterministic cases such as incomplete scientific notation (`10^`) when adjacent lines provide exactly one safe exponent hint; if the hint set conflicts or stays incomplete, the segment must remain in review.
- narration fast v2 still routes unresolved hard cases to stable codes such as `technical_term_uncertainty`, `ambiguous_term`, or `low_confidence_gate`; the cost-saving lane must abstain and review, not guess.
- New codes should be documented here when they become stable enough for fixtures/tests.
