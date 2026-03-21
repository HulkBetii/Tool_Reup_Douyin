# Known Limitations

This file tracks current limitations so future bugfixes do not confuse "not implemented yet" with regressions.

## Current known limits

1. No automatic diarization / speaker clustering pipeline is wired end-to-end.
2. Speaker -> voice preset binding va voice policy hien van la manual; chua co diarization auto hay speaker cluster -> character binding.
3. Prompt templates are seeded from code into project-local preset files, not managed from a single repo prompt catalog directory.
4. Golden semantic datasets da co semantic-QC manifest va review-gate manifest, nhung do phu van mong so voi cac edge case thuc te.
5. Some real-world sample resolution/polish scripts live under [scripts](C:\Users\HulkBeoti\Documents\Reup_Video\scripts) and are useful for exploration, but they are not a substitute for fixture-driven regression tests.
6. Register-aware voice style da co layer rieng cho `speed/volume/pitch`, nhung chua co emotion modeling sau hon, auto voice casting, hoac policy semantic tinh vi hon theo actor/persona.
7. Release hardening hien moi o muc local Windows personal-use:
   - doctor/preflight co the block dung stage
   - workspace backup/repair/cache ops da co
   - nhung chua co transactional rollback day du hay auto dependency installer
8. Packaging smoke da co headless doctor mode va checklist, nhung van can manual validation tren bundle/installer truoc khi ship cho nguoi khac.
9. Clean-machine validation kit hien tai moi cover `bundle first`:
   - da co script chuan bi kit + report contract
   - installer validation van la wave tiep theo sau khi bundle pass tren may sach

## Important distinction

- A limitation should route to review or conservative behavior.
- A regression is when behavior becomes worse than intended contract or prior guard.

## Current contract reminders

- `allowed_alternates` in relationship memory now supports both:
  - legacy flat lists, which apply to both sides
  - side-specific dictionaries such as `self_terms` and `address_terms`
- Refactors to semantic QC should preserve the behavior that whitelisted alternates remain safe even under `locked_by_human` relations.
- Speaker binding should preserve three fail-safe rules:
  - no saved bindings => global preset path
  - active bindings + unresolved recognized speaker => block TTS/export
  - `unknown_*` placeholder speakers => fallback to global preset
- Voice policy should preserve six precedence/fail-safe rules:
  - explicit speaker binding wins over voice policy
  - relationship policy wins over character policy
  - missing preset in selected policy source blocks instead of silently falling back
  - no matching policy is safe to fall back to the global preset
  - style-only policies are valid and must not be treated as unbound
  - relationship style overrides beat character style overrides field-by-field
- Register-aware voice style should preserve four rules:
  - it only affects `speed/volume/pitch`, not preset selection
  - it sits below relationship/character style overrides
  - `needs_human_review` or weak speaker/relation confidence skips register-aware style
  - missing register style policy is a safe fallback, not a blocker
