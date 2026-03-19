# AI Bugfix Workflow

This document defines the default bugfix workflow for AI-assisted changes in this repo.

## Goal

Turn real-world failures into durable regression knowledge.

The target outcome is not "the sample looks fixed".
The target outcome is:

- reproducible bug
- identified root cause
- correct-layer fix
- regression guard
- safe failover to review when ambiguity remains

## Mandatory sequence

1. Reproduce
   - Extract the smallest fixture that still reproduces the bug.
   - Prefer JSON/YAML/text fixture over a large binary sample.
   - Anonymize if needed.

2. Classify
   - Map the bug to a class in [ERROR_TAXONOMY.md](C:\Users\HulkBeoti\Documents\Reup_Video\docs\ERROR_TAXONOMY.md).
   - If needed, add a new class.

3. Root cause
   - Identify the primary faulty layer:
     - input/context
     - schema/contract
     - memory persistence
     - semantic inference
     - adaptation
     - QC severity
     - review routing
     - gate
     - mapping/binding
     - UI state
     - export/render

4. Write a failing test before the fix
   - Unit test for a deterministic rule when possible.
   - Integration test for stage interactions.
   - Semantic regression test for real zh->vi discourse failures.

5. Fix the smallest correct layer
   - Do not hardcode to one literal sentence unless it is glossary policy.
   - Prefer deterministic logic for deterministic rules.
   - Use prompt/schema changes only when the failure is actually inference-related.

6. Add regression guard
   - semantic QC rule
   - invariant
   - confidence threshold
   - locked memory rule
   - review queue routing
   - TTS/export gate

7. Audit blast radius
   - List at least 3 nearby patterns likely affected.
   - Propose follow-up tests.

8. Update docs/spec
   - taxonomy
   - QC spec
   - prompt contract
   - known limitations
   - fixture manifest

## Safety requirements

- Do not let TTS/export continue when semantic state is unsafe.
- Do not silently invent speaker/listener or honorific decisions under weak evidence.
- If confidence is too low or evidence conflicts, route to review.

## Fixture rules

- Put regression fixtures under [tests/fixtures/regression](C:\Users\HulkBeoti\Documents\Reup_Video\tests\fixtures\regression)
- Put stable golden reference fixtures under [tests/fixtures/golden](C:\Users\HulkBeoti\Documents\Reup_Video\tests\fixtures\golden)
- Register new fixtures in [tests/fixtures/manifest.json](C:\Users\HulkBeoti\Documents\Reup_Video\tests\fixtures\manifest.json)

## Required outputs for every meaningful bugfix

- failing fixture
- failing test
- fix
- passing test
- guard
- blast-radius note
- fail-safe note

