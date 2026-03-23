# Codebase Map

This is a practical working map for regression-first debugging.

## Stack

- Python 3.10+
- PySide6 desktop UI
- SQLite project database
- FFmpeg / ffprobe
- faster-whisper
- OpenAI Structured Outputs
- VieNeu / SAPI for TTS

## Runtime entrypoints

- launcher: [main.py](C:\Users\HulkBeoti\Documents\Reup_Video\main.py)
- app bootstrap: [src/app/main.py](C:\Users\HulkBeoti\Documents\Reup_Video\src\app\main.py)

## Main module ownership

- core
  - settings, logging, ffmpeg/path helpers, job orchestration
  - key files:
    - [src/app/core/settings.py](C:\Users\HulkBeoti\Documents\Reup_Video\src\app\core\settings.py)
    - [src/app/core/jobs.py](C:\Users\HulkBeoti\Documents\Reup_Video\src\app\core\jobs.py)

- project
  - project bootstrap, workspace layout, SQLite schema, runtime restore, reusable project profiles
  - key files:
    - [src/app/project/bootstrap.py](C:\Users\HulkBeoti\Documents\Reup_Video\src\app\project\bootstrap.py)
    - [src/app/project/database.py](C:\Users\HulkBeoti\Documents\Reup_Video\src\app\project\database.py)
    - [src/app/project/models.py](C:\Users\HulkBeoti\Documents\Reup_Video\src\app\project\models.py)
    - [src/app/project/profiles.py](C:\Users\HulkBeoti\Documents\Reup_Video\src\app\project\profiles.py)
    - [src/app/project/runtime_state.py](C:\Users\HulkBeoti\Documents\Reup_Video\src\app\project\runtime_state.py)

- ops
  - environment doctor, workspace backup/repair, cache inventory/cleanup, bundle smoke helpers
  - key files:
    - [src/app/ops/doctor.py](C:\Users\HulkBeoti\Documents\Reup_Video\src\app\ops\doctor.py)
    - [src/app/ops/project_safety.py](C:\Users\HulkBeoti\Documents\Reup_Video\src\app\ops\project_safety.py)
    - [src/app/ops/cache_ops.py](C:\Users\HulkBeoti\Documents\Reup_Video\src\app\ops\cache_ops.py)
    - [src/app/ops/release_validation.py](C:\Users\HulkBeoti\Documents\Reup_Video\src\app\ops\release_validation.py)
    - [scripts/smoke_release_bundle.ps1](C:\Users\HulkBeoti\Documents\Reup_Video\scripts\smoke_release_bundle.ps1)

- media
  - ffprobe and audio extraction cache
  - key files:
    - [src/app/media/ffprobe_service.py](C:\Users\HulkBeoti\Documents\Reup_Video\src\app\media\ffprobe_service.py)
    - [src/app/media/extract_audio.py](C:\Users\HulkBeoti\Documents\Reup_Video\src\app\media\extract_audio.py)

- asr
  - ASR engine abstraction and persistence
  - key files:
    - [src/app/asr/faster_whisper_engine.py](C:\Users\HulkBeoti\Documents\Reup_Video\src\app\asr\faster_whisper_engine.py)
    - [src/app/asr/persistence.py](C:\Users\HulkBeoti\Documents\Reup_Video\src\app\asr\persistence.py)

- translate
  - prompt templates, OpenAI calls, contextual V2 runtime, scene chunking, semantic QC
  - key files:
    - [src/app/translate/models.py](C:\Users\HulkBeoti\Documents\Reup_Video\src\app\translate\models.py)
    - [src/app/translate/presets.py](C:\Users\HulkBeoti\Documents\Reup_Video\src\app\translate\presets.py)
    - [src/app/translate/openai_engine.py](C:\Users\HulkBeoti\Documents\Reup_Video\src\app\translate\openai_engine.py)
    - [src/app/translate/contextual_pipeline.py](C:\Users\HulkBeoti\Documents\Reup_Video\src\app\translate\contextual_pipeline.py)
    - [src/app/translate/contextual_runtime.py](C:\Users\HulkBeoti\Documents\Reup_Video\src\app\translate\contextual_runtime.py)
    - [src/app/translate/scene_chunker.py](C:\Users\HulkBeoti\Documents\Reup_Video\src\app\translate\scene_chunker.py)
    - [src/app/translate/semantic_qc.py](C:\Users\HulkBeoti\Documents\Reup_Video\src\app\translate\semantic_qc.py)
  - runtime note:
    - [src/app/translate/contextual_runtime.py](C:\Users\HulkBeoti\Documents\Reup_Video\src\app\translate\contextual_runtime.py) is also the hook for stage-batch resilience. It should split batches not only for mismatched ids, but also for retryable structured-output parse failures on large scene batches.
    - narration routing now happens per-scene inside [src/app/translate/contextual_runtime.py](C:\Users\HulkBeoti\Documents\Reup_Video\src\app\translate\contextual_runtime.py): narration-like scenes use deterministic planner + positional structured outputs, while dialogue/borderline scenes fall back to the full dialogue path.
    - [src/app/translate/openai_engine.py](C:\Users\HulkBeoti\Documents\Reup_Video\src\app\translate\openai_engine.py) now builds stable `prompt_cache_key` values and uses a fixed prompt section order (`constraints -> context -> glossary -> source`) to maximize prompt-cache reuse.

- subtitle
  - editor helpers, subtitle QC, preview, SRT/ASS export, hard-sub rendering
  - key files:
    - [src/app/subtitle/editing.py](C:\Users\HulkBeoti\Documents\Reup_Video\src\app\subtitle\editing.py)
    - [src/app/subtitle/qc.py](C:\Users\HulkBeoti\Documents\Reup_Video\src\app\subtitle\qc.py)
    - [src/app/subtitle/export.py](C:\Users\HulkBeoti\Documents\Reup_Video\src\app\subtitle\export.py)
    - [src/app/subtitle/hardsub.py](C:\Users\HulkBeoti\Documents\Reup_Video\src\app\subtitle\hardsub.py)

- tts / audio
  - TTS engines, voice presets, speaker binding, voice policy, stage hashing, voice track building, mixdown
  - key files:
    - [src/app/tts/pipeline.py](C:\Users\HulkBeoti\Documents\Reup_Video\src\app\tts\pipeline.py)
    - [src/app/tts/presets.py](C:\Users\HulkBeoti\Documents\Reup_Video\src\app\tts\presets.py)
    - [src/app/tts/speaker_binding.py](C:\Users\HulkBeoti\Documents\Reup_Video\src\app\tts\speaker_binding.py)
    - [src/app/tts/vieneu_engine.py](C:\Users\HulkBeoti\Documents\Reup_Video\src\app\tts\vieneu_engine.py)
    - [src/app/audio/voiceover_track.py](C:\Users\HulkBeoti\Documents\Reup_Video\src\app\audio\voiceover_track.py)
    - [src/app/audio/mixdown.py](C:\Users\HulkBeoti\Documents\Reup_Video\src\app\audio\mixdown.py)
  - runtime note:
    - [src/app/tts/pipeline.py](C:\Users\HulkBeoti\Documents\Reup_Video\src\app\tts\pipeline.py) must preserve or probe cached clip duration metadata. If cached `duration_ms` collapses to `0`, [src/app/audio/voiceover_track.py](C:\Users\HulkBeoti\Documents\Reup_Video\src\app\audio\voiceover_track.py) will hard-trim clips to slot length and cut sentences mid-speech.
    - [src/app/tts/pipeline.py](C:\Users\HulkBeoti\Documents\Reup_Video\src\app\tts\pipeline.py) now reuses per-clip shared TTS cache under `cache/tts/clips/`, so narration reruns can avoid resynthesizing unchanged segments even when the overall stage hash changes.
    - narration-only incremental audio helpers live in [src/app/audio/narration_incremental.py](C:\Users\HulkBeoti\Documents\Reup_Video\src\app\audio\narration_incremental.py).

- ui
  - end-user workflow, review queue, gates, manual repair entrypoints
  - key files:
    - [src/app/ui/main_window.py](C:\Users\HulkBeoti\Documents\Reup_Video\src\app\ui\main_window.py)
    - [src/app/ui/status_panel.py](C:\Users\HulkBeoti\Documents\Reup_Video\src\app\ui\status_panel.py)

## Data model snapshot

Canonical project state is centered in [src/app/project/database.py](C:\Users\HulkBeoti\Documents\Reup_Video\src\app\project\database.py):

- `projects`
- `media_assets`
- `segments`
- `subtitle_tracks`
- `subtitle_events`
- `job_runs`
- `character_profiles`
- `relationship_profiles`
- `scene_memories`
- `segment_analyses`
- `speaker_bindings`
- `voice_policies`

Important distinction:

- `segments` = canonical output layer used by downstream pipeline
- `subtitle_tracks` / `subtitle_events` = editable track layer
- `segment_analyses` = contextual semantic truth layer for review/QC

## Current data flow

1. source video selected in project bootstrap
2. ffprobe writes `media_assets`
3. audio extraction caches 16k/48k artifacts
4. ASR writes `segments`
5. translation runs in one of two modes:
   - legacy: direct translation output -> `segments`
   - contextual_v2: scene planner -> semantic pass -> dialogue adaptation -> semantic QC -> `segment_analyses`
6. contextual outputs are applied back into canonical `segments`
7. canonical outputs sync into canonical subtitle track
8. user edits live in user subtitle tracks
9. speaker binding / voice policy resolve per-segment `voice preset` and optional style overrides
10. TTS uses subtitle rows / `tts_text`
11. voice track + mixdown produce audio artifacts
12. export uses active subtitle track + optional mixed audio
13. ops layer can preflight/block, backup, repair stale metadata, and prune orphan cache without changing semantic content

Narration incremental rerun v1:

- eligible only when the project/profile is narration-oriented and no speaker binding / voice policy layer is active
- builds:
  - `voice_scene_chunk(scene_id)`
  - `mixed_scene_chunk(scene_id)`
  - `final_audio_track`
  - `visual_base`
  - `final_mux`
- this path is wired through [scripts/rerun_contextual_downstream.py](C:\Users\HulkBeoti\Documents\Reup_Video\scripts\rerun_contextual_downstream.py)
- if scene coverage is missing or not contiguous, runtime falls back to the legacy full rerun path

## Reusable project profiles

- available profiles live under `presets/project_profiles/*.json`
- applied profile state lives under `workspace/.ops/project_profile_state.json`
- current built-in narration profile:
  - `zh-vi-narration-clear-vieneu`
  - intended for science/exploration/wilderness narration
  - applies:
    - `vieneu-default-vi speed = 0.93`
    - `default-ass FontSize = 12`
    - recommended downstream mix default `original_volume = 0.07`
- key files:
  - [src/app/project/profiles.py](C:\Users\HulkBeoti\Documents\Reup_Video\src\app\project\profiles.py)
  - [docs/PROJECT_PROFILES.md](C:\Users\HulkBeoti\Documents\Reup_Video\docs\PROJECT_PROFILES.md)

## Best hook points for durable bugfixes

### Regression fixtures

- semantic fixture root:
  - [tests/fixtures/regression](C:\Users\HulkBeoti\Documents\Reup_Video\tests\fixtures\regression)
- good source objects to snapshot:
  - contextual scene payloads
  - segment analysis rows
  - review queue rows
  - subtitle rows before TTS/export

### Semantic QC

- primary rule module:
  - [src/app/translate/semantic_qc.py](C:\Users\HulkBeoti\Documents\Reup_Video\src\app\translate\semantic_qc.py)
- persistence/gating bridge:
  - [src/app/translate/contextual_pipeline.py](C:\Users\HulkBeoti\Documents\Reup_Video\src\app\translate\contextual_pipeline.py)

### Confidence / review gate

- review status + QC persistence:
  - [src/app/project/database.py](C:\Users\HulkBeoti\Documents\Reup_Video\src\app\project\database.py)
- UI review surface:
  - [src/app/ui/main_window.py](C:\Users\HulkBeoti\Documents\Reup_Video\src\app\ui\main_window.py)

### subtitle_text / tts_text invariants

- adaptation output schema:
  - [src/app/translate/models.py](C:\Users\HulkBeoti\Documents\Reup_Video\src\app\translate\models.py)
- adaptation model call:
  - [src/app/translate/openai_engine.py](C:\Users\HulkBeoti\Documents\Reup_Video\src\app\translate\openai_engine.py)
- semantic QC invariant checks:
  - [src/app/translate/semantic_qc.py](C:\Users\HulkBeoti\Documents\Reup_Video\src\app\translate\semantic_qc.py)

### Character / relationship / scene memory

- DB schema and records:
  - [src/app/project/models.py](C:\Users\HulkBeoti\Documents\Reup_Video\src\app\project\models.py)
  - [src/app/project/database.py](C:\Users\HulkBeoti\Documents\Reup_Video\src\app\project\database.py)
- planning/runtime use:
  - [src/app/translate/contextual_runtime.py](C:\Users\HulkBeoti\Documents\Reup_Video\src\app\translate\contextual_runtime.py)

### Speaker -> voice preset binding

- persistence:
  - [src/app/project/database.py](C:\Users\HulkBeoti\Documents\Reup_Video\src\app\project\database.py)
  - `speaker_bindings` table
- planning / normalization:
  - [src/app/tts/speaker_binding.py](C:\Users\HulkBeoti\Documents\Reup_Video\src\app\tts\speaker_binding.py)
- UI:
  - [src/app/ui/main_window.py](C:\Users\HulkBeoti\Documents\Reup_Video\src\app\ui\main_window.py)
- downstream script/runtime:
  - [scripts/rerun_contextual_downstream.py](C:\Users\HulkBeoti\Documents\Reup_Video\scripts\rerun_contextual_downstream.py)

Current contract:
- no saved bindings => global preset behavior
- active bindings + unresolved recognized speaker => fail-safe block
- `unknown_*` placeholder speakers => fallback to global preset

### Voice policy theo nhan vat / quan he

- persistence:
  - [src/app/project/database.py](C:\Users\HulkBeoti\Documents\Reup_Video\src\app\project\database.py)
  - `voice_policies` table
- resolver:
  - [src/app/tts/speaker_binding.py](C:\Users\HulkBeoti\Documents\Reup_Video\src\app\tts\speaker_binding.py)
- UI:
  - [src/app/ui/main_window.py](C:\Users\HulkBeoti\Documents\Reup_Video\src\app\ui\main_window.py)
- downstream script/runtime:
  - [scripts/rerun_contextual_downstream.py](C:\Users\HulkBeoti\Documents\Reup_Video\scripts\rerun_contextual_downstream.py)

Current precedence:
- explicit speaker binding
- relationship voice policy (`speaker -> listener`)
- character voice policy
- global preset

Effective style precedence:
- relationship voice policy `speed/volume/pitch`
- character voice policy `speed/volume/pitch`
- register-aware voice style policy (`register/tone/turn-function/relation_type`)
- selected preset defaults

Fail-safe contract:
- missing preset inside the selected binding/policy source => block
- unresolved recognized speaker still blocks when explicit speaker binding mode is active
- unknown placeholder speakers do not trigger manual policy/binding requirements
- style-only policies are valid; they may override `speed/volume/pitch` without changing preset

### Register-aware voice style policy

- persistence:
  - [src/app/project/database.py](C:\Users\HulkBeoti\Documents\Reup_Video\src\app\project\database.py)
  - `register_voice_style_policies` table
- resolver:
  - [src/app/tts/speaker_binding.py](C:\Users\HulkBeoti\Documents\Reup_Video\src\app\tts\speaker_binding.py)
- UI:
  - [src/app/ui/main_window.py](C:\Users\HulkBeoti\Documents\Reup_Video\src\app\ui\main_window.py)
- downstream script/runtime:
  - [scripts/rerun_contextual_downstream.py](C:\Users\HulkBeoti\Documents\Reup_Video\scripts\rerun_contextual_downstream.py)

Current contract:
- match keys:
  - `politeness`
  - `power_direction`
  - `emotional_tone`
  - `turn_function`
  - `relation_type`
- precedence:
  - relationship style
  - character style
  - register-aware style
  - preset defaults
- safety:
  - register-aware style never changes preset directly
  - `needs_human_review=true` or weak speaker/relation evidence => skip register-aware style
  - missing register style policy is safe and falls back to the higher layers above
- changing effective per-segment style must invalidate TTS cache the same way changing preset does

### Release hardening / ops

- doctor and preflight:
  - [src/app/ops/doctor.py](C:\Users\HulkBeoti\Documents\Reup_Video\src\app\ops\doctor.py)
  - [src/app/main.py](C:\Users\HulkBeoti\Documents\Reup_Video\src\app\main.py) (`--doctor-report`)
  - [src/app/ui/main_window.py](C:\Users\HulkBeoti\Documents\Reup_Video\src\app\ui\main_window.py)
- project safety:
  - [src/app/ops/project_safety.py](C:\Users\HulkBeoti\Documents\Reup_Video\src\app\ops\project_safety.py)
  - workspace-local `.ops/backups`
- cache hygiene:
  - [src/app/ops/cache_ops.py](C:\Users\HulkBeoti\Documents\Reup_Video\src\app\ops\cache_ops.py)
- release smoke:
  - [scripts/smoke_release_bundle.ps1](C:\Users\HulkBeoti\Documents\Reup_Video\scripts\smoke_release_bundle.ps1)
  - [scripts/prepare_clean_machine_validation.py](C:\Users\HulkBeoti\Documents\Reup_Video\scripts\prepare_clean_machine_validation.py)
  - [scripts/finalize_clean_machine_validation.py](C:\Users\HulkBeoti\Documents\Reup_Video\scripts\finalize_clean_machine_validation.py)
  - [docs/RELEASE_CHECKLIST.md](C:\Users\HulkBeoti\Documents\Reup_Video\docs\RELEASE_CHECKLIST.md)

Current contract:
- ops data lives under `workspace/.ops/`
- doctor only blocks stages that actually depend on the missing tool/key
- backup happens before high-value reruns, not before every tiny edit
- repair only clears stale artifact references; it does not rewrite semantic text
- cache cleanup keeps artifacts still referenced by `runtime_state` or `job_runs`

## Current weak spots for regression-oriented development

- prompt templates are still seeded from code and project presets, not from a committed prompt catalog directory
- golden/regression fixture catalog exists, but coverage is still thin compared with the number of real-world semantic edge cases
- helper scripts used in real reruns need explicit regression tests whenever they gain new gating logic
