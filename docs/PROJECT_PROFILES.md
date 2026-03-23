# Project Profiles

Project profiles dong goi mot bo cai dat tai su dung cho workspace moi hoac project da co.

Current built-in profile:

- `zh-vi-narration-clear-vieneu`
  - video thuyet minh/kham pha `zh -> vi`
  - `translation_mode = contextual_v2`
  - prompt khuyen nghi: `contextual_default_adaptation`
  - active voice preset: `vieneu-default-vi`
  - override `VieNeu speed = 0.93`
  - override `default-ass FontSize = 12`
  - downstream mix khuyen nghi:
    - `original_volume = 0.07`
    - `voice_volume = 1.0`
- `zh-vi-narration-fast-vieneu`
  - video thuyet minh dai, it hoi thoai
  - `translation_mode = contextual_v2`
  - prompt khuyen nghi: `contextual_narration_fast_adaptation`
  - active voice preset: `vieneu-default-vi`
  - override `VieNeu speed = 0.93`
  - override `default-ass FontSize = 12`
  - downstream mix khuyen nghi:
    - `original_volume = 0.07`
    - `voice_volume = 1.0`
  - fast path runtime:
    - route theo `scene`, khong ep ca video di cung mot duong
    - scene narration ro rang se di `Narration Fast Path`
    - scene hoi thoai hoac borderline se fallback sang dialogue path day du
    - narration batches dung structured output theo vi tri, khong phu thuoc `segment_id`
    - bo qua LLM scene planner cho scene narration
    - bo qua semantic critic cho scene narration
    - dung batch semantic/adaptation lon hon
    - neu batch narration bi under-return, runtime se tu ha batch cap cho cac batch narration con lai thay vi tiep tuc retry batch lon lap lai
    - gui context/glossary nhe hon de giam token va review gia cho narration videos
    - co `prompt_cache_key` on dinh theo family/role/model/route/profile de giam input token lap lai
  - downstream narration:
    - uu tien incremental rerun khi khong co speaker binding / voice policy
    - tach `visual_base` va `final_mux`
    - tach audio theo `scene-level chunks`
    - subtitle-only edit co the reuse TTS/audio
    - audio-only edit co the reuse `visual_base` va mux lai bang `-c:v copy`

Workspace layout:

- available profiles: `presets/project_profiles/*.json`
- applied profile state: `.ops/project_profile_state.json`

Current behavior:

- bootstrap project moi co san profile files
- neu tao project voi `project_profile_id`, profile se duoc apply ngay vao preset files + active project settings
- `rerun_contextual_downstream.py` se tu dung `recommended_original_volume` / `recommended_voice_volume` tu profile state neu khong override tay

Useful commands:

```powershell
python .\scripts\apply_project_profile.py --project-root <project-root> --project-profile-id zh-vi-narration-clear-vieneu
```

```powershell
python .\scripts\run_contextual_v2_headless.py --input-video <video> --project-root <project-root> --project-profile-id zh-vi-narration-clear-vieneu
```

```powershell
python .\scripts\run_contextual_v2_headless.py --input-video <video> --project-root <project-root> --project-profile-id zh-vi-narration-fast-vieneu
```

Design note:

- profile nay co y tong quat hoa tu cac video narration khoa hoc/kham pha/hoang da, khong gan cung vao mot sample cu the
- neu can style moi, them profile moi thay vi sua tay tung project roi nho bang mieng
