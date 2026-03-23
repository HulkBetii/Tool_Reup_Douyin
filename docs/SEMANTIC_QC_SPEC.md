# Semantic QC Spec

This document records the intended role of semantic QC before TTS/export.

## Current implementation location

- rules: [src/app/translate/semantic_qc.py](C:\Users\HulkBeoti\Documents\Reup_Video\src\app\translate\semantic_qc.py)
- QC recompute + persistence: [src/app/translate/contextual_pipeline.py](C:\Users\HulkBeoti\Documents\Reup_Video\src\app\translate\contextual_pipeline.py)
- review UI: [src/app/ui/main_window.py](C:\Users\HulkBeoti\Documents\Reup_Video\src\app\ui\main_window.py)

## Purpose

Semantic QC is the last deterministic safety layer between:

- contextual translation outputs
- human review queue
- TTS/export

It should catch classes of failure that are dangerous even when the text is fluent.

## Current rule families

- `low_confidence_gate`
- `addressee_mismatch`
- `pronoun_without_evidence`
- `sub_tts_pronoun_divergence`
- `honorific_drift`
- `directionality_mismatch`

## Core invariants

1. `subtitle_text` and `tts_text` should preserve the same discourse stance.
2. Honorific policy should not drift inside the same speaker/listener pair without evidence.
3. Pronoun or vocative insertion under weak listener evidence should not silently pass.
4. Locked relation memory should constrain honorific choices.
5. Unsafe semantic outputs must not proceed to TTS/export.
6. Pronoun/vocative detection must work across punctuation boundaries such as `em,` or `anh...`.
7. If `tts_text` adds honorific policy that `subtitle_text` does not carry, and listener/discourse evidence is weak, QC should raise an error rather than a warning.
8. If relationship memory is `locked_by_human` or otherwise confirmed, a self/address-term mismatch against that relation should raise an error rather than a warning.
9. `allowed_alternates` under a locked/confirmed relation acts as a whitelist override for valid alternate self/address terms; those alternates must not be blocked as `directionality_mismatch`.
10. When `allowed_alternates` is side-specific, it must only relax the intended side:
    - `self_terms` may relax `self_term` only
    - `address_terms` may relax `address_term` only
    - legacy flat lists continue to mean "allowed on both sides"
11. Narration-safe rows must not inherit stale default audience honorific policy if `subtitle_text` and `tts_text` are both neutral.
12. Narration rows must not let `tts_text` inject audience-address terms such as `quy vi`, `cac ban`, or `moi nguoi` on only one side.

## Fail-safe rule

If the system cannot be confident enough about:

- speaker
- listener
- relation
- honorific policy
- key semantic referent

then the output should prefer:

- conservative wording
- `needs_human_review = true`
- semantic QC issue

instead of a confident but risky guess.

For narration videos in particular, this means:

- incomplete fragments stay in review
- technical-term uncertainty stays in review until a human closes the term
- neutral narration should remain neutral unless the audience address is explicit in both subtitle and TTS

## Recommended future additions

- speaker/listener consistency across adjacent reply pairs
- scene memory drift checks
- unresolved proper noun tracking
- locked policy override detection
- future speaker -> voice preset mismatch checks

## Review reason code expectations

Review reason codes should be:

- normalized
- stable
- reusable across fixtures and test assertions

Normalization currently lives in:
- [src/app/translate/contextual_runtime.py](C:\Users\HulkBeoti\Documents\Reup_Video\src\app\translate\contextual_runtime.py)
