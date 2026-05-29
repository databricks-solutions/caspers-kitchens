"""
Generate Casper's-branded PNG thumbnails for Databricks Apps.

Databricks Apps expose a single visual via ``w.apps.update_app_thumbnail``
(``AppThumbnail.thumbnail`` is base64-encoded image bytes). The same image
shows up as both the card thumbnail in Discover and the small icon in the
app launcher, so it needs to read well at small sizes.

Style:
  - 512x512 PNG (re-rendered by the platform at smaller sizes)
  - Casper's red diagonal gradient background (#E8341A -> #C42B12)
  - One large centered emoji as the focal element
  - Optional small label text along the bottom (off by default — the
    platform displays the app name underneath the thumbnail anyway)

The output is intentionally tiny (a couple of KB) so passing it through
the JSON API is cheap.

Usage::

    from utils.app_thumbnails import thumbnail_b64
    from databricks.sdk.service.apps import AppThumbnail
    w.apps.update_app_thumbnail(
        APP_NAME,
        AppThumbnail(thumbnail=thumbnail_b64(emoji="🤖")),
    )

Pillow is pre-installed on the Databricks runtime — no extra install
needed. Custom packages would have to come through the workspace's
allowed package proxy, so we deliberately stick to what ships with the
runtime here.
"""

from __future__ import annotations

import base64
import io
from typing import Optional

CASPERS_RED = (232, 52, 26)        # --accent in apps/caspers-ops-dashboard/index.html
CASPERS_RED_DARK = (196, 43, 18)   # --accent in dark theme
CASPERS_NAVY = (27, 49, 57)        # --db-dark


def _gradient(size: int, top: tuple[int, int, int], bottom: tuple[int, int, int]):
    """Build a vertical 2-stop gradient as a PIL Image."""
    from PIL import Image

    img = Image.new("RGB", (1, size))
    for y in range(size):
        t = y / max(size - 1, 1)
        img.putpixel(
            (0, y),
            tuple(int(top[i] + (bottom[i] - top[i]) * t) for i in range(3)),
        )
    return img.resize((size, size))


def _emoji_font(emoji_size: int):
    """Best-effort lookup of a colour-emoji-capable font across platforms."""
    from PIL import ImageFont

    candidates = [
        "/System/Library/Fonts/Apple Color Emoji.ttc",       # macOS
        "/usr/share/fonts/truetype/noto/NotoColorEmoji.ttf", # Debian/Ubuntu (Databricks runtime)
        "/usr/share/fonts/noto/NotoColorEmoji.ttf",
        "/usr/share/fonts/google-noto-emoji/NotoColorEmoji.ttf",
    ]
    for path in candidates:
        try:
            return ImageFont.truetype(path, size=emoji_size)
        except OSError:
            continue
    return ImageFont.load_default()


def generate_png(
    emoji: str,
    *,
    size: int = 512,
    top: tuple[int, int, int] = CASPERS_RED,
    bottom: tuple[int, int, int] = CASPERS_RED_DARK,
    label: Optional[str] = None,
) -> bytes:
    """Render a thumbnail and return the PNG bytes."""
    from PIL import Image, ImageDraw

    img = _gradient(size, top, bottom).convert("RGBA")
    draw = ImageDraw.Draw(img)

    # Color-emoji bitmap fonts (Apple/Noto) only render at 109px on Apple and
    # 136px on Noto — Pillow will scale the resulting bitmap, but the source
    # size has to be one the font actually ships. 137 is safe enough for both.
    font = _emoji_font(emoji_size=137)
    try:
        bbox = draw.textbbox((0, 0), emoji, font=font, embedded_color=True)
    except TypeError:
        bbox = draw.textbbox((0, 0), emoji, font=font)
    w, h = bbox[2] - bbox[0], bbox[3] - bbox[1]
    draw.text(
        ((size - w) / 2 - bbox[0], (size - h) / 2 - bbox[1] - size * 0.04),
        emoji,
        font=font,
        embedded_color=True,
    )

    if label:
        from PIL import ImageFont
        try:
            label_font = ImageFont.truetype("/System/Library/Fonts/Helvetica.ttc", 36)
        except OSError:
            label_font = ImageFont.load_default()
        bbox = draw.textbbox((0, 0), label, font=label_font)
        lw = bbox[2] - bbox[0]
        draw.text(
            ((size - lw) / 2, size - 80),
            label,
            font=label_font,
            fill=(255, 255, 255, 230),
        )

    buf = io.BytesIO()
    img.convert("RGB").save(buf, format="PNG", optimize=True)
    return buf.getvalue()


def thumbnail_b64(emoji: str, **kwargs) -> str:
    """Convenience wrapper returning a base64 string for ``AppThumbnail.thumbnail``."""
    return base64.b64encode(generate_png(emoji, **kwargs)).decode("ascii")
