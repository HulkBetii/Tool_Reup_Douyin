from __future__ import annotations

import json
from pathlib import Path

from app.tts.models import VoicePreset
from app.tts.presets import (
    batch_import_voice_clone_presets,
    delete_voice_preset,
    list_voice_presets,
    save_voice_preset,
)


def test_list_voice_presets_loads_json_files(tmp_path: Path) -> None:
    presets_dir = tmp_path / "presets" / "voices"
    presets_dir.mkdir(parents=True)
    (presets_dir / "default_voice.json").write_text(
        """
        {
          "voice_preset_id": "default-sapi",
          "name": "Windows SAPI Default",
          "engine": "sapi",
          "voice_id": "default",
          "speed": 1.0,
          "volume": 1.0,
          "pitch": 0.0,
          "sample_rate": 22050,
          "engine_options": {
            "mode": "local"
          }
        }
        """,
        encoding="utf-8",
    )

    presets = list_voice_presets(tmp_path)

    assert len(presets) == 1
    assert presets[0].engine == "sapi"
    assert presets[0].sample_rate == 22050
    assert presets[0].engine_options["mode"] == "local"


def test_save_voice_preset_persists_vieneu_clone_fields(tmp_path: Path) -> None:
    preset = VoicePreset(
        voice_preset_id="vieneu-clone-template",
        name="VieNeu Voice Clone",
        engine="vieneu",
        voice_id="default",
        sample_rate=24000,
        language="vi",
        engine_options={
            "mode": "local",
            "ref_audio_path": "assets/voices/sample.wav",
            "ref_text": "Xin chao day la mau tham chieu",
        },
        notes="clone",
    )

    path = save_voice_preset(tmp_path, preset)
    payload = json.loads(path.read_text(encoding="utf-8"))
    presets = list_voice_presets(tmp_path)

    assert path.exists()
    assert payload["engine_options"]["ref_audio_path"] == "assets/voices/sample.wav"
    assert payload["engine_options"]["ref_text"] == "Xin chao day la mau tham chieu"
    assert presets[0].voice_preset_id == "vieneu-clone-template"


def test_delete_voice_preset_removes_matching_file(tmp_path: Path) -> None:
    preset = VoicePreset(
        voice_preset_id="custom-voice",
        name="Custom Voice",
        engine="sapi",
    )
    path = save_voice_preset(tmp_path, preset)

    deleted_path = delete_voice_preset(tmp_path, "custom-voice")

    assert deleted_path == path
    assert not path.exists()
    assert list_voice_presets(tmp_path) == []


def test_batch_import_voice_clone_presets_uses_sidecar_text_files(tmp_path: Path) -> None:
    voices_dir = tmp_path / "assets" / "voices"
    voices_dir.mkdir(parents=True)
    (voices_dir / "narrator_a.wav").write_bytes(b"wav")
    (voices_dir / "narrator_a.txt").write_text("Xin chao day la mau A", encoding="utf-8")
    (voices_dir / "narrator_b.wav").write_bytes(b"wav")
    (voices_dir / "narrator_b.txt").write_text("Xin chao day la mau B", encoding="utf-8")
    (voices_dir / "missing_text.wav").write_bytes(b"wav")

    report = batch_import_voice_clone_presets(
        tmp_path,
        template_preset=VoicePreset(
            voice_preset_id="vieneu-template",
            name="VieNeu Clone",
            engine="vieneu",
            sample_rate=24000,
            language="vi",
            engine_options={"mode": "local", "api_base": "http://127.0.0.1:8000"},
        ),
    )
    presets = list_voice_presets(tmp_path)

    assert len(report.imported_presets) == 2
    assert report.skipped_missing_text == [voices_dir / "missing_text.wav"]
    assert report.skipped_empty_text == []
    assert [preset.voice_preset_id for preset in presets] == [
        "vieneu-clone-narrator-a",
        "vieneu-clone-narrator-b",
    ]
    assert Path(str(presets[0].engine_options["ref_audio_path"])) == Path("assets/voices/narrator_a.wav")
    assert presets[0].engine_options["ref_text"] == "Xin chao day la mau A"
    assert presets[1].engine_options["api_base"] == "http://127.0.0.1:8000"
