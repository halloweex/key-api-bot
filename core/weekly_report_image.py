"""The weekly report as a light card: one number, one chart, three stats.

The text report says the week landed at z = −0.5 and asks to be taken on
faith. The card shows it — thirteen weekly bars with the ±1σ band drawn across
them and the reported week last in the row, its top inside the band or outside
it. Where a week sits among the ones before it is the one thing a column of
numbers cannot express, and it is what decides whether a 25% drop is news.

Everything else stays in the caption. A card that repeats the message it is
attached to is one more thing to read, not one less.

Pillow only, deliberately. Matplotlib would draw this in a tenth of the code
and add ~100 MB to an image that already runs against a memory ceiling, for one
picture a week. Everything here is rectangles and text.

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
from typing import List, Optional, Sequence, Tuple

from core.weekly_report import (
    ANOMALY_Z,
    MIN_BASELINE_WEEKS,
    WeeklyReport,
    pct_change,
)

logger = logging.getLogger(__name__)

# ─── Canvas ─────────────────────────────────────────────────────────────────

W, H = 1200, 960
PAD = 72

BG = (255, 255, 255)
CHIP = (241, 245, 249)
BAND = (238, 242, 248)
LINE = (226, 232, 240)
BAR = (203, 213, 225)

TEXT = (15, 23, 42)
MUTED = (100, 116, 139)

UP = (22, 163, 74)
DOWN = (220, 38, 38)
CALM = (37, 99, 235)

CHART_TOP, CHART_BOTTOM = 380, 640

# Debian's fonts-dejavu-core, then matplotlib's bundled copy for dev machines.
# DejaVu carries Cyrillic and ₴; it does not carry colour emoji, which is why
# this card is drawn from triangles and bars and the emoji stay in the caption.
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


def _tone(report: WeeklyReport) -> Tuple[int, int, int]:
    """The card's accent: green high, red low, blue for an ordinary week.

    Tied to the verdict, not to the sign of the change. A 25% fall that sits
    inside its own normal range is painted calm, because that is what it is —
    colouring every dip red is the visual form of crying wolf, and the chip
    under the headline already carries the direction.
    """
    z = report.z
    if z is not None and abs(z) >= ANOMALY_Z:
        return UP if z > 0 else DOWN
    return CALM


def _delta(pct: Optional[float]) -> Tuple[str, Tuple[int, int, int]]:
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


def _draw_chart(d, report: WeeklyReport, fonts: _Fonts) -> None:
    """Thirteen weeks as bars, with the band this one had to land inside."""
    series: List[float] = list(report.baseline_series) + [report.current.revenue]
    if len(series) < 2:
        return

    peak = max(series) or 1.0
    slot = (W - 2 * PAD) / len(series)
    bar_w = max(14, int(slot * 0.58))

    def y_for(value: float) -> float:
        return CHART_BOTTOM - (value / peak) * (CHART_BOTTOM - CHART_TOP)

    # The ±1σ band, behind the bars, so a bar ending inside it reads as
    # ordinary without a word being spent on saying so.
    mean, sd = report.baseline_mean, report.baseline_sd
    if (mean is not None and sd is not None and sd > 0
            and report.baseline_weeks >= MIN_BASELINE_WEEKS):
        d.rectangle((PAD, y_for(mean + sd), W - PAD, y_for(max(mean - sd, 0))),
                    fill=BAND)

    accent = _tone(report)
    for i, value in enumerate(series):
        x = PAD + i * slot + (slot - bar_w) / 2
        h = (value / peak) * (CHART_BOTTOM - CHART_TOP)
        d.rounded_rectangle((x, CHART_BOTTOM - h, x + bar_w, CHART_BOTTOM),
                            radius=min(8, bar_w // 2),
                            fill=accent if i == len(series) - 1 else BAR)

    # Four words under the chart, naming what the band and the last bar say.
    z = report.z
    if z is None:
        note = "not enough history to judge"
    elif abs(z) < ANOMALY_Z:
        note = f"normal range   ·   z {z:+.1f}   ·   band ±1σ"
    else:
        note = (f"unusually {'high' if z > 0 else 'low'}   ·   "
                f"z {z:+.1f}   ·   band ±1σ")
    d.text((PAD, CHART_BOTTOM + 30), note, font=fonts.label, fill=MUTED)


def _draw_stats(d, report: WeeklyReport, fonts: _Fonts) -> None:
    """Orders, basket, and the split that says which of them moved."""
    cur, prev = report.current, report.previous
    top = 762
    d.line((PAD, top - 40, W - PAD, top - 40), fill=LINE, width=2)

    def cell(x: float, label: str, value: str,
             deltas: Sequence[Optional[float]]) -> None:
        d.text((x, top), label, font=fonts.label, fill=MUTED)
        d.text((x, top + 38), value, font=fonts.stat, fill=TEXT)
        # Two numbers in a cell need two deltas beside them; one would leave
        # the reader guessing which of them it belongs to.
        cursor = x
        for i, delta in enumerate(deltas):
            if i:
                d.text((cursor, top + 104), " / ", font=fonts.label, fill=LINE)
                cursor += d.textlength(" / ", font=fonts.label)
            text, colour = _delta(delta)
            d.text((cursor, top + 104), text, font=fonts.label, fill=colour)
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
        _draw_chart(d, report, fonts)
        _draw_stats(d, report, fonts)

        buf = io.BytesIO()
        img.save(buf, format="PNG", optimize=True)
        return buf.getvalue()
    except Exception as exc:
        logger.warning("Weekly report card failed to render: %s", exc, exc_info=True)
        return None
