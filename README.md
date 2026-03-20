# Reup Video

Desktop app Windows cho workflow dich, tao phu de, long tieng va xuat video chay local.

Tai lieu goc trong workspace chon stack MVP:

- Python 3.x
- PySide6
- libmpv / python-mpv
- FFmpeg / ffprobe
- faster-whisper
- OpenAI API cho translation / rewrite

Slice da duoc khoi tao trong repo nay:

- Skeleton project theo `src/app`
- Dependency workflow bang `pyproject.toml` + `requirements*.txt`
- App settings luu JSON trong `%APPDATA%/ReupVideo` voi `OpenAI API key` duoc ma hoa
- Logging chung + file log theo `job_id`
- Bootstrap project folder + SQLite schema v0
- Job manager nen cho progress / cancel / retry
- Main window PySide6 voi tab skeleton tieng Viet
- Status panel de theo doi tien trinh jobs
- Detect FFmpeg / ffprobe co ban
- Probe source video bang ffprobe va luu metadata vao MediaAsset
- Extract audio 16k / 48k vao cache theo stage hash
- ASR interface + faster-whisper wrapper + persist segments vao DB/cache
- Prompt template loader cho translation
- OpenAI translation engine dung Responses API + Structured Outputs
- Persist translation vao `segments` + cache JSON
- Export subtitle `.srt` / `.ass` tu active subtitle track
- Subtitle editor MVP de sua timing / translated text / subtitle text / tts text
- Video export bang FFmpeg voi ca hard-sub va soft-sub mux tu active subtitle track
- Subtitle QC MVP: overlap, duration, CPS, CPL, empty text
- Shift all, find/replace va preview ASS qua libmpv khi da cau hinh `mpv_dll_path`
- Split selected / merge with next trong Sub Editor
- VoicePreset loader + Windows SAPI fallback TTS
- VieNeu engine optional cho Vietnamese local/remote TTS
- VieNeu voice cloning preset editor (ref audio + ref text) trong UI
- Batch voice profile management: sua field preset, save-as-new, xoa, va import hang loat clone refs tu `assets/voices`
- Contextual Translation V2 cho `zh -> vi`: scene planner -> semantic pass -> dialogue adaptation -> semantic QC -> review gate
- Regression-first workflow: fixture manifest, semantic/golden fixtures, review reason codes, va docs cho bugfix durable
- Build voice track aligned timeline tu TTS clips
- Mixdown MVP: voice + original + BGM optional
- Video export tu dong dung mixed audio neu co
- ExportPreset loader + preset mac dinh `YouTube 16:9` va `Shorts 9:16`
- Watermark/logo profiles reusable trong tab `Xuat ban`
- Canonical `segments` tach rieng khoi `SubtitleTrack` / `SubtitleEvent`
- Canonical track duoc sync tu ASR / translation, editor save se fork sang user track rieng
- Active `VoicePreset` / `ExportPreset` duoc persist theo project
- Active `WatermarkProfile` duoc persist theo project
- Preview mpv auto-reload subtitle theo debounce khi dang sua Sub Editor
- Khoi phuc lai TTS manifest / voice track / mixed audio / subtitle outputs / export output khi mo lai project
- Workflow nhanh de chain `Prepare media`, `ASR -> Dich`, `Long tieng nhanh`, `Full pipeline`
- Manual speaker -> voice preset binding theo project, co fail-safe gate truoc TTS/export va fallback an toan cho placeholder speaker `unknown_*`
- Voice policy theo nhan vat/quan he: relationship override > character fallback > preset mac dinh, van ton trong speaker binding la muc uu tien cao nhat
- Voice style policy theo nhan vat/quan he: co the override `speed/volume/pitch` theo tung speaker hoac speaker->listener ma khong can doi preset
- PyInstaller spec + PowerShell build scripts + Inno Setup script cho ban Windows may sach

## Cau truc

```text
src/app/
  core/         # settings, logging, ffmpeg, jobs
  project/      # bootstrap project folder + SQLite schema
  media/        # ffprobe + extract audio cache
  asr/          # ASR abstraction + faster-whisper + persistence
  translate/    # prompt presets, contextual V2 runtime, semantic QC
  subtitle/     # subtitle editor helpers, QC, preview, export
  tts/          # TTS engines, voice presets, speaker binding, voice policy
  audio/        # voice track + mixdown
  ui/           # main window, tabs, status panel
tests/          # unit + integration + regression fixtures
docs/           # roadmap / notes
build/          # pyinstaller.spec va metadata build
scripts/        # build bundle / installer
installer/      # Inno Setup script
```

## Cai dat

1. Cai Python 3.10-3.12 va them vao `PATH`.
2. Tao virtualenv.
3. Cai dependencies:

```powershell
pip install -e .
pip install -r requirements-dev.txt
```

4. Chay app:

```powershell
python main.py
```

## Kiem thu

```powershell
pytest
```

## Dong Goi

Bundle Windows khong can Python:

```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\build_pyinstaller.ps1 -Clean
```

Installer `.exe` bang Inno Setup:

```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\build_installer.ps1 -BuildBundle
```

Ghi chu packaging:

- Build script se auto copy dependency runtime neu tim thay:
  - `ffmpeg.exe` / `ffprobe.exe` tu `%APPDATA%\ReupVideo\settings.json`
  - `mpv-2.dll` tu `%APPDATA%\ReupVideo\settings.json`
  - `eSpeak NG` tu `%LOCALAPPDATA%\Programs\eSpeak NG`
- Trong bundle, app tu dong auto-detect dependency duoc dong kem tai `dependencies\ffmpeg`, `dependencies\mpv`, `dependencies\espeak-ng`.
- Neu muon override, co the truyen `-FfmpegBinDir`, `-MpvDllPath`, `-EspeakRoot` cho `build_pyinstaller.ps1`.
- `build_installer.ps1` can `ISCC.exe` cua Inno Setup 6; neu khong nam o vi tri mac dinh, truyen them `-IsccPath`.

## Trang thai

Repo hien da vuot qua MVP nen tang va da co mot workflow dung that tren may local:

- media ingest + ASR + persist canonical `segments`
- Contextual Translation V2 cho `zh -> vi`
- semantic QC + review queue + fail-safe gate truoc TTS/export
- subtitle track layer tach khoi canonical `segments`
- subtitle editor practical tools + mpv preview auto-reload
- TTS local/VieNeu + voice track + mixdown + hard-sub/soft-sub export
- preset persistence cho voice/export/watermark
- manual speaker -> voice preset binding theo project
- voice policy theo nhan vat/quan he tren nen speaker binding
- voice style/prosody policy theo nhan vat/quan he (`speed/volume/pitch`) tren nen voice policy
- regression harness cho semantic bugs:
  - fixture manifest
  - regression/golden fixtures
  - error taxonomy
  - review reason codes
  - AI bugfix workflow docs
- packaging/CI co ban cho may Windows local

De preview mpv tren Windows, co the cau hinh `mpv_dll_path` trong tab `Cai dat` hoac dong kem `dependencies\mpv\mpv-2.dll` trong bundle.
Khi dang mo `Preview tu dau` hoac `Preview dong chon`, editor se tu dong export `live_preview.ass` theo debounce va reload vao mpv, nen sua subtitle se thay doi nhanh hon ma khong can mo lai preview moi lan.
TTS local hien tai dung Windows SAPI fallback. Tren may smoke hien co 2 voice English: `Microsoft David Desktop - English (United States)` va `Microsoft Zira Desktop - English (United States)`.
De dung VieNeu cho TTS tieng Viet, cai them package `vieneu` va eSpeak NG. Theo docs chinh thuc tren Windows CPU, lenh cai package la `pip install vieneu --extra-index-url https://pnnbao97.github.io/llama-cpp-python-v0.3.16/cpu/`, va eSpeak NG nen cai bang file `.msi`.
Project moi se co san preset `VieNeu Voice Clone`; ban co the vao tab `Long tieng & Audio`, chon preset nay, dien `ref audio` + `ref text`, roi bam `Luu voice preset`.
Tab `Long tieng & Audio` hien da cho sua truc tiep `name/engine/voice id/language/sample rate/speed/volume/pitch/notes`, `Save as new`, `Xoa preset`, va `Batch import refs`. Batch import se quet `assets/voices` va tao nhieu VieNeu clone presets tu cac file audio co sidecar `.txt` cung ten.
Tab `Xuat ban` hien da cho chon preset xuat, override `burn subtitles` cho tung lan export, va quan ly watermark/logo bang reusable profile. Project moi se co san `Khong watermark` va `Logo top-right`; ban co the sua logo path/opacity/scale/position roi bam `Luu profile` hoac `Save as new`.
Khi sua subtitle trong editor, lan luu dau tien tren canonical track se fork sang `Edited Subtitle Track`, de ASR / translation van tiep tuc ghi vao canonical `segments` + canonical subtitle track.
Khi mo lai project, app se tu restore cac artifact da tao truoc do tu lich su job runs, nen neu da co `voice track`, `mixed audio`, `SRT/ASS`, hoac `export video`, ban co the di tiep thay vi render lai tu dau.
Tab `Du an` hien co `Pipeline checklist` de bao dang thieu buoc nao, va `Workflow nhanh` de chain mot so cum buoc pho bien den tan export video.

Huong tiep theo hop ly:

- mo rong golden semantic dataset tu cac bug that da gap
- them regression test truc tiep cho cac helper/script downstream quan trong
- polish review UI / bulk actions cho semantic va voice policy
- xac dinh contract cho `voice style` theo register/role thay vi chi map theo character/relationship
