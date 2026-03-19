from __future__ import annotations

from pathlib import Path

from app.media.ffprobe_service import parse_ffprobe_payload


def test_parse_ffprobe_payload_extracts_video_and_audio_metadata(tmp_path: Path) -> None:
    payload = {
        "format": {
            "duration": "12.345",
            "bit_rate": "1234567",
            "size": "987654",
            "format_name": "mov,mp4,m4a,3gp,3g2,mj2",
        },
        "streams": [
            {
                "index": 0,
                "codec_type": "video",
                "codec_name": "h264",
                "width": 1920,
                "height": 1080,
                "avg_frame_rate": "30000/1001",
            },
            {
                "index": 1,
                "codec_type": "audio",
                "codec_name": "aac",
                "channels": 2,
                "sample_rate": "48000",
                "tags": {"language": "vi"},
            },
        ],
    }

    metadata = parse_ffprobe_payload(payload, tmp_path / "input.mp4", sha256="abc123")

    assert metadata.duration_ms == 12345
    assert metadata.bit_rate == 1234567
    assert metadata.size_bytes == 987654
    assert metadata.width == 1920
    assert metadata.height == 1080
    assert metadata.primary_audio_stream is not None
    assert metadata.primary_audio_stream.channels == 2
    assert metadata.primary_audio_stream.sample_rate == 48000
    assert metadata.sha256 == "abc123"

