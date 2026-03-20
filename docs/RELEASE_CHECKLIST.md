# Release Checklist

This checklist is for local Windows bundle / installer validation.

## Before build

1. Run `ruff check src tests scripts`.
2. Run `pytest -q`.
3. Open one real project and ensure:
   - `pending_review_count = 0`
   - semantic gate is clean
   - TTS/export still works on current machine

## Build bundle

1. Build PyInstaller bundle:
   - `powershell -ExecutionPolicy Bypass -File .\scripts\build_pyinstaller.ps1 -Clean`
2. If needed, build installer:
   - `powershell -ExecutionPolicy Bypass -File .\scripts\build_installer.ps1 -BuildBundle`

## Smoke the bundle

1. Run bundle smoke:
   - `powershell -ExecutionPolicy Bypass -File .\scripts\smoke_release_bundle.ps1`
2. Verify the bundle can generate a doctor report.
3. Check that bundled dependencies are present when expected:
   - `dependencies\ffmpeg\ffmpeg.exe`
   - `dependencies\ffmpeg\ffprobe.exe`
   - `dependencies\mpv\mpv-2.dll`
   - `dependencies\espeak-ng\`

## Functional validation on a sample project

1. Launch the built app.
2. Open a sample project copy.
3. Run `Doctor` from `Cai dat` or `Du an`.
4. Verify `Workspace safety` is clean enough for rerun.
5. Run a short downstream rerun:
   - `TTS`
   - `Track giong`
   - `Tron am thanh`
   - `Xuat video`
6. Confirm:
   - backup was created under `workspace\.ops\backups`
   - cache cleanup does not remove referenced artifacts
   - output video is created successfully

## Do not ship if

- doctor reports blocking errors for the stage you need
- workspace repair reports schema errors
- semantic review queue is still non-zero for a contextual project
- TTS/export succeeds only after manual file copying outside the documented dependency paths
