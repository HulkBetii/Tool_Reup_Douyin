# Known Limitations

This file tracks current limitations so future bugfixes do not confuse "not implemented yet" with regressions.

## Current known limits

1. No automatic diarization / speaker clustering pipeline is wired end-to-end.
2. No dedicated speaker -> voice preset binding layer exists yet.
3. Prompt templates are seeded from code into project-local preset files, not managed from a single repo prompt catalog directory.
4. Golden semantic datasets are not committed yet; current repo relies mostly on unit/integration tests plus project-local sample runs.
5. Some real-world sample resolution/polish scripts live under [scripts](C:\Users\HulkBeoti\Documents\Reup_Video\scripts) and are useful for exploration, but they are not a substitute for fixture-driven regression tests.

## Important distinction

- A limitation should route to review or conservative behavior.
- A regression is when behavior becomes worse than intended contract or prior guard.

## Current contract reminders

- `allowed_alternates` in relationship memory now supports both:
  - legacy flat lists, which apply to both sides
  - side-specific dictionaries such as `self_terms` and `address_terms`
- Refactors to semantic QC should preserve the behavior that whitelisted alternates remain safe even under `locked_by_human` relations.
