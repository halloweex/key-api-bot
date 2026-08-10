"""The weekly report as a light card: the number, and the four that explain it.

What the week did, at a glance — revenue against the week before, then orders,
basket and the new/repeat split, which between them say *why* it moved. Nothing
else. Everything the caption already carries stays in the caption; a card that
repeats the message it is attached to is one more thing to read, not one less.

Pillow only, deliberately. Matplotlib would add ~100 MB to an image that
already runs against a memory ceiling, for one picture a week. Everything here
is rectangles and text.

Drawn at roughly twice its display size: Telegram re-encodes photos as JPEG,
and thin text on a flat background is exactly what that treatment ruins.

Nothing in this module may break the report: a missing font, an odd value, any
failure at all returns None and the job sends its text as before.
"""
from __future__ import annotations

import io
import logging
import os
from dataclasses import dataclass
from typing import Optional, Sequence, Tuple

from core.weekly_report import WeeklyReport, pct_change

logger = logging.getLogger(__name__)

# ─── Canvas ─────────────────────────────────────────────────────────────────

W, H = 1200, 640
PAD = 72

BG = (255, 255, 255)
CHIP = (241, 245, 249)
LINE = (226, 232, 240)

TEXT = (15, 23, 42)
MUTED = (100, 116, 139)

UP = (22, 163, 74)
DOWN = (220, 38, 38)

STATS_TOP = 430

# Debian's fonts-dejavu-core, then matplotlib's bundled copy for dev machines.
# DejaVu carries Cyrillic and ₴; it does not carry colour emoji, which is why
# this card is drawn from triangles and the emoji stay in the caption.
FONT_PATHS = (
    "/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf",
    "/usr/share/fonts/dejavu/DejaVuSans.ttf",
)
BOLD_PATHS = (
    "/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf",
    "/usr/share/fonts/dejavu/DejaVuSans-Bold.ttf",
)


@dataclass(frozen=True)
class _Fonts:
    label: object
    chip: object
    currency: object
    stat: object
    big: object


def _font_file(candidates: Sequence[str], matplotlib_name: str) -> Optional[str]:
    for path in candidates:
        if os.path.exists(path):
            return path
    try:  # dev machines: matplotlib ships DejaVu and is not a prod dependency
        import matplotlib
        bundled = os.path.join(
            os.path.dirname(matplotlib.__file__), "mpl-data/fonts/ttf", matplotlib_name,
        )
        if os.path.exists(bundled):
            return bundled
    except Exception:
        pass
    return None


def _load_fonts() -> Optional[_Fonts]:
    """Every size the card uses, or None if the host has no DejaVu.

    `ImageFont.load_default()` is not a fallback worth having: it is a bitmap
    face with no Cyrillic and no ₴, so it would render this catalogue as a row
    of boxes. Better to send the text report alone.
    """
    from PIL import ImageFont

    regular = _font_file(FONT_PATHS, "DejaVuSans.ttf")
    bold = _font_file(BOLD_PATHS, "DejaVuSans-Bold.ttf")
    if not regular or not bold:
        logger.info("Weekly report card skipped: DejaVu fonts not installed")
        return None

    return _Fonts(
        label=ImageFont.truetype(regular, 27),
        chip=ImageFont.truetype(bold, 30),
        currency=ImageFont.truetype(bold, 46),
        stat=ImageFont.truetype(bold, 50),
        big=ImageFont.truetype(bold, 112),
    )


# ─── Small helpers ──────────────────────────────────────────────────────────

def _money(value: float) -> str:
    # DejaVu sets ₴ tight against a following digit; a space keeps the two from
    # reading as one glyph.
    return f"₴ {value:,.0f}"


def _delta(pct: Optional[float]) -> Tuple[str, Tuple[int, int, int]]:
    """A change worth showing, or an honest mark that there is none."""
    if pct is None:
        return "—", MUTED
    if abs(pct) < 0.5:
        return "≈ flat", MUTED
    return f"{'▲' if pct > 0 else '▼'} {abs(pct):.1f}%", UP if pct > 0 else DOWN


# ─── The card ───────────────────────────────────────────────────────────────

def _draw_header(d, report: WeeklyReport, fonts: _Fonts) -> None:
    """Which week, and which book. The caption says the rest."""
    window = f"{report.start.strftime('%d.%m')} – {report.end.strftime('%d.%m.%Y')}"
    d.text((PAD, PAD), f"{window}   ·   {report.sales_type}",
           font=fonts.label, fill=MUTED)


def _draw_headline(d, report: WeeklyReport, fonts: _Fonts) -> None:
    """The number, and how it compares to the week before it."""
    cur, prev = report.current, report.previous

    # The currency mark is smaller and dimmer: at 112px bold DejaVu's ₴ is as
    # heavy as a digit, and the eye reads "₴9" as one character.
    d.text((PAD, PAD + 108), "₴", font=fonts.currency, fill=MUTED)
    d.text((PAD + 52, PAD + 58), f"{cur.revenue:,.0f}", font=fonts.big, fill=TEXT)

    text, colour = _delta(pct_change(cur.revenue, prev.revenue if prev else None))
    if prev is not None:
        text = f"{text} vs last week"
    box = d.textbbox((0, 0), text, font=fonts.chip)
    w, h = box[2] - box[0], box[3] - box[1]
    x, y = PAD, 270
    d.rounded_rectangle((x, y, x + w + 44, y + h + 32), radius=(h + 32) // 2,
                        fill=CHIP)
    d.text((x + 22, y + 14), text, font=fonts.chip, fill=colour)


def _draw_stats(d, report: WeeklyReport, fonts: _Fonts) -> None:
    """Orders, basket, and the split that says which of them moved."""
    cur, prev = report.current, report.previous
    d.line((PAD, STATS_TOP - 40, W - PAD, STATS_TOP - 40), fill=LINE, width=2)

    def cell(x: float, label: str, value: str,
             deltas: Sequence[Optional[float]]) -> None:
        d.text((x, STATS_TOP), label, font=fonts.label, fill=MUTED)
        d.text((x, STATS_TOP + 38), value, font=fonts.stat, fill=TEXT)
        # Two numbers in a cell need two deltas beside them; one would leave
        # the reader guessing which of them it belongs to.
        cursor = x
        for i, delta in enumerate(deltas):
            if i:
                d.text((cursor, STATS_TOP + 104), " / ", font=fonts.label, fill=LINE)
                cursor += d.textlength(" / ", font=fonts.label)
            text, colour = _delta(delta)
            d.text((cursor, STATS_TOP + 104), text, font=fonts.label, fill=colour)
            cursor += d.textlength(text, font=fonts.label)

    width = (W - 2 * PAD) / 3
    cell(PAD, "ORDERS", f"{cur.orders:,}",
         [pct_change(cur.orders, prev.orders) if prev else None])
    cell(PAD + width, "AVG CHECK", _money(cur.avg_check),
         [pct_change(cur.avg_check, prev.avg_check) if prev else None])

    if cur.new_customer_orders is not None:
        cell(PAD + 2 * width, "NEW / REPEAT",
             f"{cur.new_customer_orders:,} / {cur.repeat_orders:,}",
             [pct_change(cur.new_customer_orders,
                         prev.new_customer_orders if prev else None),
              pct_change(cur.repeat_orders,
                         prev.repeat_orders if prev else None)])


def render_weekly_card(report: WeeklyReport) -> Optional[bytes]:
    """The card as PNG bytes, or None if it could not be drawn.

    Never raises. A card is a nice-to-have wrapped around numbers that are not,
    so every failure here degrades to the text report rather than costing the
    week its message.
    """
    try:
        from PIL import Image, ImageDraw

        fonts = _load_fonts()
        if fonts is None:
            return None

        img = Image.new("RGB", (W, H), BG)
        d = ImageDraw.Draw(img)

        _draw_header(d, report, fonts)
        _draw_headline(d, report, fonts)
        _draw_stats(d, report, fonts)

        buf = io.BytesIO()
        img.save(buf, format="PNG", optimize=True)
        return buf.getvalue()
    except Exception as exc:
        logger.warning("Weekly report card failed to render: %s", exc, exc_info=True)
        return None
