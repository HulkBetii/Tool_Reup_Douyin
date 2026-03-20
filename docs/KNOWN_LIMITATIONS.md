# Known Limitations

This file tracks current limitations so future bugfixes do not confuse "not implemented yet" with regressions.

## Current known limits

1. No automatic diarization / speaker clustering pipeline is wired end-to-end.
2. Speaker -> voice preset binding hien la manual va character-based; chua co diarization auto hay speaker cluster -> character binding.
3. Prompt templates are seeded from code into project-local preset files, not managed from a single repo prompt catalog directory.
4. Golden semantic datasets da co khung/fixture commit, nhung do phu van mong so voi cac edge case thuc te.
5. Some real-world sample resolution/polish scripts live under [scripts](C:\Users\HulkBeoti\Documents\Reup_Video\scripts) and are useful for exploration, but they are not a substitute for fixture-driven regression tests.
6. Voice policy theo quan he/nhan vat chua duoc tach thanh mot layer rieng; hien speaker binding moi map speaker -> preset, khong map register/role -> style.

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
