"""The weekly report as a card that animates once and holds.

The text report says the week landed at z = −0.5. The card *shows* it: thirteen
weeks as bars with the ±1σ band drawn across them, and the reported week the
last bar in the row. Where a week sits among the ones before it is the one
thing a column of numbers cannot express, and it is the thing that decides
whether a 25% drop is news.

The animation earns its place by carrying that same argument in time — the
bars rise oldest-first while the headline counts up, so the eye arrives at the
last bar already knowing the shape it has to fit into. It is not a number
spinning for its own sake.

Pillow only, deliberately. Matplotlib would draw this in a tenth of the code
and add ~100 MB to an image that already runs against a memory ceiling, for
one chart a week. Every frame here is rectangles and text.

Nothing in this module is allowed to break the report: a missing font, an odd
value, any failure at all returns None and the job sends its text as before.
"""
from __future__ import annotations

import io
import logging
import math
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

W, H = 720, 700
PAD = 44

BG = (18, 22, 28)
SURFACE = (26, 32, 41)
BAND = (33, 41, 52)
TEXT = (232, 237, 244)
MUTED = (138, 151, 168)
DIM = (63, 74, 89)

UP = (61, 214, 140)
DOWN = (255, 107, 107)
CALM = (91, 156, 246)

# Frames of movement, then frames of stillness. The hold is most of the loop:
# a card that restarts the moment it finishes is a card nobody can read.
GROW_FRAMES = 26
HOLD_FRAMES = 10
FRAME_MS = 55
HOLD_MS = 220

# GIF palettes are per-file, not per-frame. Quantising every frame against one
# master palette is what keeps the background from crawling between frames.
PALETTE_COLORS = 64

# Debian's fonts-dejavu-core, then matplotlib's bundled copy for dev machines.
# DejaVu carries Cyrillic and ₴; it does not carry colour emoji, which is why
# this card is drawn from triangles and dots and the emoji stay in the caption.
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
    body: object
    stat: object
    big: object
    chip: object


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

    `ImageFont.load_default()` is not a fallback worth having here: it is a
    bitmap face with no Cyrillic and no ₴, so it would render this catalogue as
    a row of boxes. Better to send the text report alone.
    """
    from PIL import ImageFont

    regular = _font_file(FONT_PATHS, "DejaVuSans.ttf")
    bold = _font_file(BOLD_PATHS, "DejaVuSans-Bold.ttf")
    if not regular or not bold:
        logger.info("Weekly report card skipped: DejaVu fonts not installed")
        return None

    return _Fonts(
        label=ImageFont.truetype(regular, 17),
        body=ImageFont.truetype(regular, 21),
        stat=ImageFont.truetype(bold, 31),
        big=ImageFont.truetype(bold, 68),
        chip=ImageFont.truetype(bold, 20),
    )


# ─── Small helpers ──────────────────────────────────────────────────────────

# DejaVu sets ₴ tight against a following digit, and at 68px bold the two
# glyphs read as one. A thin space is the typographic fix; the headline gets a
# stronger one and draws the symbol itself.
def _money(value: float) -> str:
    return f"₴ {value:,.0f}"


def _ease(t: float) -> float:
    """Ease-out cubic. Bars decelerate into place instead of stopping dead."""
    return 1 - (1 - t) ** 3


def _tone(report: WeeklyReport) -> Tuple[int, int, int]:
    """The card's accent: green high, red low, blue for an ordinary week.

    Tied to the verdict, not to the sign of the change. A 25% fall that sits
    inside its own normal range is painted calm, because that is what it is —
    colouring every dip red is the visual form of crying wolf, and the chip
    above already carries the direction for anyone who wants it.
    """
    z = report.z
    if z is not None and abs(z) >= ANOMALY_Z:
        return UP if z > 0 else DOWN
    return CALM


def _delta_text(pct: Optional[float]) -> Tuple[str, Tuple[int, int, int]]:
    if pct is None:
        return "no prior week", MUTED
    if abs(pct) < 0.5:
        return "≈ flat vs last week", MUTED
    arrow = "▲" if pct > 0 else "▼"
    return f"{arrow} {abs(pct):.1f}% vs last week", UP if pct > 0 else DOWN


# ─── The card ───────────────────────────────────────────────────────────────

def _draw_frame(report: WeeklyReport, fonts: _Fonts, progress: float):
    """One frame at `progress` in [0, 1]. At 1.0 this is the finished card."""
    from PIL import Image, ImageDraw

    img = Image.new("RGB", (W, H), BG)
    d = ImageDraw.Draw(img)
    accent = _tone(report)
    cur, prev = report.current, report.previous

    # ── Header ──
    d.text((PAD, PAD), "WEEKLY REPORT", font=fonts.label, fill=MUTED)
    window = f"{report.start.strftime('%d.%m')} – {report.end.strftime('%d.%m.%Y')}"
    d.text((W - PAD, PAD), f"{window}  ·  {report.sales_type}",
           font=fonts.label, fill=MUTED, anchor="ra")

    # ── Headline, counted up ──
    # The currency mark is drawn separately, smaller and dimmer: at 68px bold
    # DejaVu's ₴ is as heavy as a digit and the eye reads "₴9" as one glyph.
    shown = cur.revenue * _ease(min(progress / 0.75, 1.0))
    d.text((PAD, PAD + 76), "₴", font=fonts.stat, fill=MUTED)
    d.text((PAD + 34, PAD + 44), f"{shown:,.0f}", font=fonts.big, fill=TEXT)

    if progress > 0.75:
        text, colour = _delta_text(
            pct_change(cur.revenue, prev.revenue if prev else None)
        )
        box = d.textbbox((0, 0), text, font=fonts.chip)
        w, h = box[2] - box[0], box[3] - box[1]
        x, y = PAD, PAD + 132
        d.rounded_rectangle((x, y, x + w + 28, y + h + 20), radius=(h + 20) // 2,
                            fill=SURFACE)
        d.text((x + 14, y + 9), text, font=fonts.chip, fill=colour)

    # ── The chart ──
    _draw_chart(d, report, fonts, accent, progress)

    # ── Stats ──
    if progress > 0.85:
        _draw_stats(d, report, fonts)

    return img


def _draw_chart(d, report: WeeklyReport, fonts: _Fonts, accent, progress: float) -> None:
    """Thirteen weeks as bars, with the band this one had to land inside."""
    top, bottom = 246, 470
    series: List[float] = list(report.baseline_series) + [report.current.revenue]
    if len(series) < 2:
        return

    peak = max(series) or 1.0
    slot = (W - 2 * PAD) / len(series)
    bar_w = max(8, int(slot * 0.62))

    def y_for(value: float) -> float:
        return bottom - (value / peak) * (bottom - top)

    # The ±1σ band, drawn behind the bars so a bar inside it reads as ordinary.
    mean, sd = report.baseline_mean, report.baseline_sd
    show_band = (
        mean is not None and sd is not None and sd > 0
        and report.baseline_weeks >= MIN_BASELINE_WEEKS
        and progress > 0.55
    )
    if show_band:
        d.rectangle((PAD, y_for(mean + sd), W - PAD, y_for(max(mean - sd, 0))),
                    fill=BAND)
        d.line((PAD, y_for(mean), W - PAD, y_for(mean)), fill=DIM, width=1)

    # Bars rise oldest-first, each starting a little after the one before it,
    # so the row reads left to right the way the weeks happened.
    for i, value in enumerate(series):
        last = i == len(series) - 1
        start = (i / len(series)) * 0.55
        local = _ease(max(0.0, min((progress - start) / 0.42, 1.0)))
        if local <= 0:
            continue

        x = PAD + i * slot + (slot - bar_w) / 2
        full_h = (value / peak) * (bottom - top)
        h = full_h * local
        colour = accent if last else DIM
        d.rounded_rectangle((x, bottom - h, x + bar_w, bottom),
                            radius=min(5, bar_w // 2), fill=colour)

    # Verdict, under the row it refers to. The band is named here rather than
    # labelled on the chart, where the tag collided with the newest bar.
    if progress > 0.9:
        z = report.z
        if z is None:
            line = "not enough history to judge"
        elif abs(z) < ANOMALY_Z:
            line = f"inside the normal range   ·   z {z:+.1f}   ·   band ±1σ"
        else:
            line = (f"unusually {'high' if z > 0 else 'low'}   ·   "
                    f"z {z:+.1f}   ·   band ±1σ")
        d.text((PAD, bottom + 22), line, font=fonts.body, fill=MUTED)


def _draw_stats(d, report: WeeklyReport, fonts: _Fonts) -> None:
    """Orders, basket, and the split that says which of them moved."""
    cur, prev = report.current, report.previous
    top = 540
    d.line((PAD, top - 18, W - PAD, top - 18), fill=SURFACE, width=2)

    def chunk(delta: Optional[float]) -> Tuple[str, Tuple[int, int, int]]:
        if delta is None:
            return "—", MUTED
        if abs(delta) < 0.5:
            return "≈ flat", MUTED
        return (f"{'▲' if delta > 0 else '▼'} {abs(delta):.0f}%",
                UP if delta > 0 else DOWN)

    def cell(x: float, label: str, value: str,
             deltas: Sequence[Optional[float]]) -> None:
        d.text((x, top), label, font=fonts.label, fill=MUTED)
        d.text((x, top + 26), value, font=fonts.stat, fill=TEXT)
        # Two numbers in a cell need two deltas beside them; one would leave
        # the reader guessing which of them it belongs to.
        cursor = x
        for i, delta in enumerate(deltas):
            if i:
                d.text((cursor, top + 70), " / ", font=fonts.label, fill=DIM)
                cursor += d.textlength(" / ", font=fonts.label)
            text, colour = chunk(delta)
            d.text((cursor, top + 70), text, font=fonts.label, fill=colour)
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


# ─── Assembly ───────────────────────────────────────────────────────────────

def render_weekly_gif(report: WeeklyReport) -> Optional[bytes]:
    """The animated card as GIF bytes, or None if it could not be drawn.

    Never raises. A card is a nice-to-have wrapped around numbers that are not,
    so every failure here degrades to the text report rather than costing the
    week its message.
    """
    try:
        fonts = _load_fonts()
        if fonts is None:
            return None

        frames = [
            _draw_frame(report, fonts, (i + 1) / GROW_FRAMES)
            for i in range(GROW_FRAMES)
        ]
        durations = [FRAME_MS] * GROW_FRAMES
        frames += [frames[-1]] * HOLD_FRAMES
        durations += [HOLD_MS] * HOLD_FRAMES

        # One palette for the whole file, taken from the finished frame — it
        # holds every colour the animation ever shows.
        master = frames[-1].quantize(colors=PALETTE_COLORS)
        quantised = [f.quantize(palette=master, dither=0) for f in frames]

        buf = io.BytesIO()
        quantised[0].save(
            buf, format="GIF", save_all=True, append_images=quantised[1:],
            duration=durations, loop=0, optimize=True, disposal=1,
        )
        return buf.getvalue()
    except Exception as exc:
        logger.warning("Weekly report card failed to render: %s", exc, exc_info=True)
        return None
