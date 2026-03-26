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
    - scene narration co the chay `term/entity mini-pass` scene-level truoc semantic/adaptation
    - mini-pass tao `narration_term_sheet` nhe de giu cach goi thuat ngu/thuc the on dinh trong scene
    - neu model bo trong `segment_positions`, runtime se fallback anchor `source_term -> source_text` de review hint van bam dung segment
    - neu mini-pass danh dau mot term trung tam la `needs_review`, runtime se route dung segment do sang review thay vi doan nghia
    - narration batches dung structured output theo vi tri, khong phu thuoc `segment_id`
    - bo qua LLM scene planner cho scene narration
    - bo qua semantic critic cho scene narration
    - dung batch semantic/adaptation lon hon
    - neu batch narration bi under-return, runtime se tu ha batch cap cho cac batch narration con lai thay vi tiep tuc retry batch lon lap lai
    - gui context/glossary nhe hon de giam token va review gia cho narration videos
    - co `prompt_cache_key` on dinh theo family/role/model/route/profile de giam input token lap lai
    - contextual runs dai se checkpoint partial theo `scene` trong cache, nen neu mat ket noi giua chung co the resume tu scene da xong thay vi mat sach progress
- `zh-vi-narration-fast-v2-vieneu`
  - video thuyet minh dai, uu tien cost/throughput
  - `translation_mode = contextual_v2`
  - prompt khuyen nghi: `contextual_narration_slot_rewrite`
  - active voice preset: `vieneu-default-vi`
  - override `VieNeu speed = 0.93`
  - override `default-ass FontSize = 12`
  - downstream mix khuyen nghi:
    - `original_volume = 0.07`
    - `voice_volume = 1.0`
  - narration v2 runtime:
    - route theo `scene`, chi co 2 lane: `narration_fast_v2` va `dialogue_legacy`
    - narration scenes lien tiep duoc gom thanh `span` de giam call/token
    - base path chi chay `1 semantic pass` va sinh `canonical_text`
    - `subtitle_text = tts_text = canonical_text` theo mac dinh, khong chay default dialogue adaptation
    - escalation la sparse:
      - `entity_micro_pass`
      - `ambiguity_micro_pass`
      - `slot_rewrite`
    - co `NarrationBudgetPolicy` voi soft-stop de giu estimated LLM cost trong budget
    - co run-local / project-local / global narration term memory
  - subtitle subtext:
    - project state co `subtitle_subtext_mode = off | source_text`
    - mac dinh `off`
    - UI co toggle `Subtext gốc`
    - toggle chi anh huong preview/export/hardsub, khong anh huong TTS hay semantic QC
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
- UI co `Chế độ giao diện`:
  - `Đơn giản (V2)` la mac dinh
  - mode nay uu tien tao project moi bang profile `zh-vi-narration-fast-v2-vieneu`
  - an cac form/policy/ops nang va giu lai flow chinh: tao project -> ASR & Dịch -> review -> Phụ đề -> TTS -> track giọng -> trộn -> xuất
  - co the bat lai `Nâng cao` bat cu luc nao de hien toan bo lane cu
- simple mode tu goi y ten project / thu muc project theo video da chon, dung workspace root on dinh thay vi `cwd`, va mac dinh `zh -> vi`, `ASR = zh`, mix narration `goc = 0.07`, `giong = 1.0`, `BGM = 0.0`
- simple mode cung rut gon quy trinh nhanh tren tab `Du an` thanh 4 hanh dong ro hon: `1. Chuan bi video`, `2. Tao phu de`, `3. Mo review`, `4. Hoan thien video`
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

```powershell
python .\scripts\run_contextual_v2_headless.py --input-video <video> --project-root <project-root> --project-profile-id zh-vi-narration-fast-v2-vieneu
```

```powershell
python .\scripts\resume_contextual_v2_project.py --project-root <project-root>
```

Design note:

- profile nay co y tong quat hoa tu cac video narration khoa hoc/kham pha/hoang da, khong gan cung vao mot sample cu the
- neu can style moi, them profile moi thay vi sua tay tung project roi nho bang mieng
