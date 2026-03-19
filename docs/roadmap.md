# Roadmap Slice

Tai lieu `.docx` trong workspace de xuat trinh tu bat dau nhu sau:

1. `FND-01` Repo skeleton + Hello UI
2. `FND-02` Dependency workflow + requirements
3. `FND-03` Logging chuan + per-job logs
4. `FND-04` AppSettings JSON
5. `CORE-01` Project folder layout initializer
6. `CORE-02` SQLite schema v0
7. `UI-01` MainWindow tab skeleton tieng Viet
8. `UI-02` Status/progress panel + cancel wiring
9. `JOB-01` JobManager
10. `MEDIA-01` FFmpeg detect + version check

Repo hien tai implement dung slice tren de tao baseline cho cac stage ASR, translation, subtitle editor, TTS va export.

Slice tiep theo da duoc implement them:

11. `MEDIA-02` ffprobe metadata -> MediaAsset
12. `MEDIA-03` Extract audio 16k + 48k vao cache
13. `ASR-01` ASR engine interface + models
14. `ASR-02` faster-whisper engine wrapper
15. `ASR-03` Persist ASR segments vao DB + JSON cache
16. Translation prompt template loader
17. OpenAI translation engine with Structured Outputs
18. Persist translated text vao canonical segments
19. Export subtitle SRT / ASS tu segments da dich
20. Subtitle editor MVP de sua timing/text va luu nguoc vao DB
21. Hard-sub export MVP bang FFmpeg tu ASS hien tai
22. Subtitle QC MVP: overlap/duration/CPS/CPL/empty text
23. Shift all + find/replace trong Sub Editor
24. Preview mpv MVP voi `mpv_dll_path`
25. Split selected / merge with next trong Sub Editor
26. TTS engine abstraction + VoicePreset loader
27. Windows SAPI fallback TTS clips
28. Voice track aligned timeline (fit-to-slot + adelay + amix)
29. Mixdown MVP: voice + original + BGM optional
30. Hard-sub export uu tien mixed audio neu co
31. ExportPreset loader + default presets 16:9 / 9:16
32. Watermark/logo override trong export tab
33. VieNeu engine adapter + bootstrap preset tieng Viet
34. Voice runtime status trong UI (SAPI + VieNeu + eSpeak NG)
35. VieNeu voice cloning preset editor + persist ref audio/ref text
36. SubtitleTrack / SubtitleEvent tach rieng khoi canonical `segments`
37. Canonical subtitle track sync tu ASR / translation, editor save auto-fork sang user track
38. Export / preview / TTS / hard-sub doi sang active subtitle track
39. Runtime auto-detect dependency dong kem trong bundle (`ffmpeg`, `mpv`, `eSpeak NG`)
40. PyInstaller spec + build script PowerShell cho bundle Windows
41. Inno Setup script + build wrapper cho installer may sach
42. `OpenAI API key` luu encrypted trong settings + migrate tu plaintext field cu
43. Active `VoicePreset` / `ExportPreset` duoc persist trong project state va restore khi mo lai
44. Tab `Xuat ban` ho tro ca burn-in ASS va soft-sub mux thong qua `ExportPreset.burn_subtitles`
45. Preview mpv reload subtitle theo debounce tu `Sub Editor` ma khong can restart video
46. Watermark/logo profiles reusable, luu JSON theo project va persist active profile trong project state
47. Restore pipeline artifact state tu job history khi mo lai project (`TTS`, `voice track`, `mixdown`, `subtitle outputs`, `export video`)
48. Them `Pipeline checklist` + `Workflow nhanh` de chain cac buoc `Prepare media`, `ASR -> Dich`, `Long tieng nhanh`, `Full pipeline`
49. Batch voice profile management: editor preset day du, save-as-new, delete, va import hang loat clone refs tu `assets/voices`
