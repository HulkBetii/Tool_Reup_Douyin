from __future__ import annotations

import subprocess
import wave
from pathlib import Path

from .base import TTSEngine
from .models import SynthesisResult, VoicePreset


def _ps_quote(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def _speed_to_sapi_rate(speed: float) -> int:
    normalized = max(0.4, min(speed, 2.0))
    return max(-10, min(10, int(round((normalized - 1.0) * 8))))


def _volume_to_sapi_value(volume: float) -> int:
    return max(0, min(100, int(round(max(0.0, min(volume, 2.0)) * 100 / 1.5))))


def build_sapi_synthesize_script(
    *,
    text: str,
    output_path: Path,
    preset: VoicePreset,
) -> str:
    output_str = str(output_path.resolve())
    lines = [
        "$ErrorActionPreference = 'Stop'",
        "$voice = New-Object -ComObject SAPI.SpVoice",
        f"$voice.Rate = {_speed_to_sapi_rate(preset.speed)}",
        f"$voice.Volume = {_volume_to_sapi_value(preset.volume)}",
    ]
    if preset.voice_id and preset.voice_id.lower() != "default":
        lines.extend(
            [
                f"$targetVoice = {_ps_quote(preset.voice_id)}",
                "$matched = $null",
                "for($i=0; $i -lt $voice.GetVoices().Count; $i++){",
                "  $token = $voice.GetVoices().Item($i)",
                "  if($token.GetDescription() -like ('*' + $targetVoice + '*')) { $matched = $token; break }",
                "}",
                "if($matched -ne $null){ $voice.Voice = $matched }",
            ]
        )
    lines.extend(
        [
            "$stream = New-Object -ComObject SAPI.SpFileStream",
            f"$stream.Open({_ps_quote(output_str)}, 3, $false)",
            "$voice.AudioOutputStream = $stream",
            f"$null = $voice.Speak({_ps_quote(text)})",
            "$stream.Close()",
        ]
    )
    return "\n".join(lines)


def list_installed_sapi_voices() -> list[str]:
    script = (
        "$voice = New-Object -ComObject SAPI.SpVoice; "
        "for($i=0; $i -lt $voice.GetVoices().Count; $i++){ "
        "$token = $voice.GetVoices().Item($i); Write-Output $token.GetDescription() }"
    )
    result = subprocess.run(
        ["powershell.exe", "-NoProfile", "-Command", script],
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
        timeout=20,
        check=False,
        creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0),
    )
    if result.returncode != 0:
        return []
    return [line.strip() for line in result.stdout.splitlines() if line.strip()]


class SapiTTSEngine(TTSEngine):
    def synthesize(
        self,
        *,
        text: str,
        output_path: Path,
        preset: VoicePreset,
    ) -> SynthesisResult:
        output_path.parent.mkdir(parents=True, exist_ok=True)
        script = build_sapi_synthesize_script(text=text, output_path=output_path, preset=preset)
        result = subprocess.run(
            ["powershell.exe", "-NoProfile", "-Command", script],
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
            timeout=120,
            check=False,
            creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0),
        )
        if result.returncode != 0 or not output_path.exists() or output_path.stat().st_size <= 44:
            raise RuntimeError((result.stderr or result.stdout or "Windows SAPI synth that bai").strip())

        with wave.open(str(output_path), "rb") as handle:
            frames = handle.getnframes()
            sample_rate = handle.getframerate()
            duration_ms = int((frames / sample_rate) * 1000) if sample_rate else 0
        return SynthesisResult(
            wav_path=output_path,
            duration_ms=duration_ms,
            sample_rate=sample_rate,
            voice_id=preset.voice_id if preset.voice_id != "default" else None,
        )
