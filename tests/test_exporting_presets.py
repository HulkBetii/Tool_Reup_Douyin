from __future__ import annotations

import json
from pathlib import Path

from app.exporting.models import WatermarkProfile
from app.exporting.presets import list_watermark_profiles, save_watermark_profile


def test_list_and_save_watermark_profiles(tmp_path: Path) -> None:
    profiles_dir = tmp_path / "presets" / "watermarks"
    profiles_dir.mkdir(parents=True)
    (profiles_dir / "none.json").write_text(
        json.dumps(
            {
                "watermark_profile_id": "watermark-none",
                "name": "Khong watermark",
                "watermark_enabled": False,
            }
        ),
        encoding="utf-8",
    )
    (profiles_dir / "logo.json").write_text(
        json.dumps(
            {
                "watermark_profile_id": "watermark-logo-top-right",
                "name": "Logo top-right",
                "watermark_enabled": True,
                "watermark_path": "assets/logos/logo.png",
                "watermark_position": "top-right",
                "watermark_opacity": 0.8,
                "watermark_scale": 0.2,
                "watermark_margin": 32,
            }
        ),
        encoding="utf-8",
    )

    profiles = list_watermark_profiles(tmp_path)

    assert [profile.watermark_profile_id for profile in profiles] == [
        "watermark-logo-top-right",
        "watermark-none",
    ]

    output_path = save_watermark_profile(
        tmp_path,
        WatermarkProfile(
            watermark_profile_id="watermark-brand-bottom-left",
            name="Brand bottom-left",
            watermark_enabled=True,
            watermark_path="assets/logos/brand.png",
            watermark_position="bottom-left",
            watermark_opacity=0.72,
            watermark_scale=0.14,
            watermark_margin=20,
            notes="Profile da luu de reuse",
        ),
    )

    payload = json.loads(output_path.read_text(encoding="utf-8"))

    assert output_path.name == "watermark-brand-bottom-left.json"
    assert payload["name"] == "Brand bottom-left"
    assert payload["watermark_position"] == "bottom-left"
