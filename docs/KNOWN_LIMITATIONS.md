# Known Limitations

This file tracks current limitations so future bugfixes do not confuse "not implemented yet" with regressions.

## Current known limits

1. No automatic diarization / speaker clustering pipeline is wired end-to-end.
2. Speaker -> voice preset binding va voice policy hien van la manual; chua co diarization auto hay speaker cluster -> character binding.
3. Prompt templates are seeded from code into project-local preset files, not managed from a single repo prompt catalog directory.
4. Golden semantic datasets da co khung/fixture commit, nhung do phu van mong so voi cac edge case thuc te.
5. Some real-world sample resolution/polish scripts live under [scripts](C:\Users\HulkBeoti\Documents\Reup_Video\scripts) and are useful for exploration, but they are not a substitute for fixture-driven regression tests.
6. Voice policy theo quan he/nhan vat da co layer rieng cho preset resolution va `speed/volume/pitch` overrides, nhung chua map register/role -> speaking style policy o muc semantic.

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
