# Error Taxonomy

This file classifies failures for regression-oriented bugfix work.

## 1. Input / Context

Definition:
- wrong or missing source context enters the stage

Examples:
- scene window omits relevant turns
- glossary/context payload is empty when required

Typical fix:
- fix payload builder, scene windowing, or fixture preparation

## 2. Schema / Contract

Definition:
- model output schema or internal data contract lacks required fields or invariants

Examples:
- no `speaker/listener` field
- no explicit `honorific_policy`
- no way to represent review state
- relation memory stores alternates too coarsely, so a self-only alternate accidentally relaxes address-side checks

Typical fix:
- update Pydantic/DB schema and callers together

## 3. Memory Persistence / Restore

Definition:
- character/relationship/scene state is not stored, restored, or locked correctly

Examples:
- relationship defaults drift after reload
- active artifacts restore but semantic state does not
- relationship status (`hypothesized` vs `locked_by_human`) is dropped before QC, so locked memory is enforced too weakly

Typical fix:
- fix DB persistence, restore logic, or locking semantics

## 4. Semantic Inference

Definition:
- speaker/listener/relation/register inference is wrong or overconfident

Examples:
- wrong speaker
- wrong listener
- pronoun inserted without evidence

Typical fix:
- prompt/schema/context changes plus confidence/review routing

## 5. Dialogue Adaptation

Definition:
- `subtitle_text` and `tts_text` diverge in meaning, honorifics, or discourse stance

Examples:
- subtitle neutral, TTS adds "em"
- TTS changes politeness level

Typical fix:
- tighten adaptation invariant or critic/QC checks

## 6. Semantic QC / Severity

Definition:
- a real semantic failure is only marked as warning or not flagged at all

Examples:
- `sub_tts_pronoun_divergence` stays warning when it should block export
- vocative or pronoun appears only in `tts_text`, but punctuation-boundary matching misses it
- TTS-only pronoun injection under ambiguous listener evidence is treated as non-blocking
- `directionality_mismatch` stays warning even when relation memory was manually locked
- neutral narration inherits stale default audience policy and gets false-positive `honorific_drift`
- narration `tts_text` injects `quy vi`/`cac ban` while subtitle stays neutral and QC misses the one-sided drift

Typical fix:
- promote severity, add new QC rule, or add invariant

## 7. Review Routing / Confidence

Definition:
- ambiguous or low-confidence outputs do not reach human review

Examples:
- `needs_human_review` stays false under weak listener evidence

Typical fix:
- confidence threshold, reason code normalization, review gate logic

## 8. Gate / Safety Enforcement

Definition:
- TTS/export can proceed even though semantic state is unsafe

Examples:
- semantic QC has issues but downstream still runs

Typical fix:
- block at canonical output, TTS stage, subtitle export, and video export

## 9. Voice / Speaker Mapping

Definition:
- wrong speaker uses wrong voice preset or wrong voiceover track

Examples:
- future speaker->voice binding mismatch

Typical fix:
- binding layer, lock semantics, speaker validation

## 10. UI / Human Review UX

Definition:
- reviewer cannot see enough context to make the correct decision

Examples:
- review panel misses scene summary or surrounding turns

Typical fix:
- improve review surface, not translation logic

## 11. Export / Render / State Staleness

Definition:
- downstream artifacts are stale or inconsistent with approved canonical data

Examples:
- video uses old mixed audio after subtitle review
- cached TTS clips lose duration metadata on rerun, so voice track fitting trims audio before the sentence is fully spoken

Typical fix:
- invalidate state, recompute expected artifact hash, block export when stale

## 12. Environment / Packaging

Definition:
- app fails because runtime dependency or packaging contract is broken

Examples:
- missing mpv DLL, ffmpeg path, PyInstaller bundle omission

Typical fix:
- detection, build script, installer, runtime diagnostics

## 13. Translation Runtime / Batch Resilience

Definition:
- a retryable LLM batch failure aborts the whole contextual run instead of degrading safely to smaller batches

Examples:
- dialogue adaptation returns truncated structured JSON on a long scene batch and the run crashes on a parse error
- stage output is structurally invalid for a large batch but succeeds when rows are retried in smaller slices

Typical fix:
- treat retryable structured-output parse/schema failures as a batch-size/runtime resilience issue
- split to smaller batches automatically
- still surface the original error once the batch is down to a single row

## Required handling rules

- Ambiguous semantic cases must fail-safe to review, not silent guessing.
- Any class-4/5/6/7/8 bug should usually produce a regression fixture.
- Any class-13 bug should usually produce a regression fixture plus a runtime retry/guard test.
- Export/TTS must not proceed through unsafe class-4/5/6/7 states.
