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

Design note:

- profile nay co y tong quat hoa tu cac video narration khoa hoc/kham pha/hoang da, khong gan cung vao mot sample cu the
- neu can style moi, them profile moi thay vi sua tay tung project roi nho bang mieng
