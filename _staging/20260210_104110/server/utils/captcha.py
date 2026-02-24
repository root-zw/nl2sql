"""
验证码生成与验证工具
"""

from __future__ import annotations

import base64
import io
import random
import string
import time
from typing import Optional, Tuple
from uuid import uuid4

from PIL import Image, ImageDraw, ImageFont, ImageFilter

CAPTCHA_TTL_SECONDS = 300
CAPTCHA_KEY_PREFIX = "admin:captcha:"
CAPTCHA_LENGTH = 4
FONT_CANDIDATES = [
    "Arial.ttf",
    "arial.ttf",
    "/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf",
]

_local_store: dict[str, Tuple[str, float]] = {}


def _cleanup_expired_entries() -> None:
    now = time.time()
    expired_keys = [
        key for key, (_, expire_ts) in _local_store.items() if expire_ts <= now
    ]
    for key in expired_keys:
        _local_store.pop(key, None)


def _load_font(size: int) -> ImageFont.FreeTypeFont:
    for font_path in FONT_CANDIDATES:
        try:
            return ImageFont.truetype(font_path, size)
        except OSError:
            continue
    return ImageFont.load_default()


def _render_captcha_image(code: str) -> str:
    width, height = 130, 48
    image = Image.new("RGB", (width, height), (255, 255, 255))
    draw = ImageDraw.Draw(image)

    # 外边框
    draw.rectangle([(0, 0), (width - 1, height - 1)], outline=(180, 180, 180))

    # 干扰线
    for _ in range(5):
        draw.line(
            [
                (random.randint(0, width), random.randint(0, height)),
                (random.randint(0, width), random.randint(0, height)),
            ],
            fill=(
                random.randint(150, 200),
                random.randint(150, 200),
                random.randint(150, 200),
            ),
            width=1,
        )

    # 干扰点
    for _ in range(80):
        x = random.randint(0, width)
        y = random.randint(0, height)
        draw.point(
            (x, y),
            fill=(
                random.randint(100, 255),
                random.randint(100, 255),
                random.randint(100, 255),
            ),
        )

    font = _load_font(30)
    spacing = width // CAPTCHA_LENGTH

    for i, char in enumerate(code):
        x = 12 + i * spacing
        y = random.randint(6, 12)
        draw.text(
            (x, y),
            char,
            font=font,
            fill=(
                random.randint(0, 100),
                random.randint(0, 100),
                random.randint(0, 100),
            ),
        )

    image = image.filter(ImageFilter.SMOOTH)

    buffer = io.BytesIO()
    image.save(buffer, format="PNG")
    return base64.b64encode(buffer.getvalue()).decode("utf-8")


def _store_code(captcha_id: str, code: str, redis_client=None) -> None:
    key = f"{CAPTCHA_KEY_PREFIX}{captcha_id}"
    value = code.lower()
    if redis_client:
        redis_client.setex(key, CAPTCHA_TTL_SECONDS, value)
        return

    _cleanup_expired_entries()
    _local_store[key] = (value, time.time() + CAPTCHA_TTL_SECONDS)


def _pop_code(captcha_id: str, redis_client=None) -> Optional[str]:
    key = f"{CAPTCHA_KEY_PREFIX}{captcha_id}"
    if redis_client:
        value = redis_client.get(key)
        if value is not None:
            redis_client.delete(key)
        return value

    _cleanup_expired_entries()
    stored = _local_store.pop(key, None)
    if not stored:
        return None
    value, expire_ts = stored
    if expire_ts <= time.time():
        return None
    return value


def generate_captcha(redis_client=None) -> Tuple[str, str]:
    code = "".join(random.choices(string.ascii_uppercase + string.digits, k=CAPTCHA_LENGTH))
    captcha_id = uuid4().hex
    _store_code(captcha_id, code, redis_client)
    image_base64 = _render_captcha_image(code)
    return captcha_id, image_base64


def validate_captcha(
    captcha_id: Optional[str],
    captcha_code: Optional[str],
    redis_client=None,
) -> bool:
    if not captcha_id or not captcha_code:
        return False

    stored = _pop_code(captcha_id, redis_client)
    if not stored:
        return False

    return stored == captcha_code.lower()

