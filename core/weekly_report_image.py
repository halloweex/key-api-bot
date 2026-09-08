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

from core import brand
from core.i18n import DEFAULT_LANGUAGE, fmt_int, fmt_money, fmt_window, normalize, t
from core.weekly_report import WeeklyReport, compact_money, decompose, pct_change

logger = logging.getLogger(__name__)

# ─── Canvas ─────────────────────────────────────────────────────────────────
#
# Colours are the brand book's (core/brand.py), used the way its p. 14 allows:
# the card is the brand's hero surface — bordeaux with lime and pink on it,
# the look of its bags, boxes and Instagram covers — and the charts sit on
# the off-white canvas with bordeaux ink. There is no red and no green in the
# palette, so a fall is never coloured red: the arrow carries the sign, and
# the colour carries the brand.

W, H = 1200, 640
PAD = 72

# The charts' canvas.
BG = brand.OFF_WHITE
LINE = brand.BEIGE
TEXT = brand.TEXT
MUTED = brand.MUTED

# The card.
CARD_BG = brand.BORDEAUX
CARD_TEXT = brand.OFF_WHITE
CARD_LABEL = brand.PINK
CHIP = brand.LIME
CHIP_TEXT = brand.BORDEAUX
CARD_LINE = brand.PLUM

# Deltas on the card: lime is the accent and marks a rise, pink a fall.
UP = brand.LIME
DOWN = brand.PINK

STATS_TOP = 430

# The brand faces first (core/brand.py says where), then Debian's
# fonts-dejavu-core, then matplotlib's bundled copy for dev machines. DejaVu
# carries Cyrillic and ₴; it does not carry colour emoji, which is why this
# card is drawn from triangles and the emoji stay in the caption.
FONT_PATHS = tuple(p for p in (
    brand.brand_font("body"),
    "/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf",
    "/usr/share/fonts/dejavu/DejaVuSans.ttf",
) if p)
BOLD_PATHS = tuple(p for p in (
    brand.brand_font("heading"),
    "/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf",
    "/usr/share/fonts/dejavu/DejaVuSans-Bold.ttf",
) if p)
ACCENT_PATHS = tuple(p for p in (brand.brand_font("accent"),) if p)

# DejaVu on its own, without the brand faces in front of it: the fallback for
# the handful of glyphs they do not carry. See `brand.FALLBACK_CHARS`.
DEJAVU_PATHS = (
    "/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf",
    "/usr/share/fonts/dejavu/DejaVuSans.ttf",
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
    if not matplotlib_name:
        return None
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


def _face(path: str, size: int, role: str):
    """`path` at `size`, set to the weight `role` asks for.

    With the brand faces installed, `body` and `heading` are the same variable
    file and only the axis tells them apart — which is why both loaders go
    through here and neither calls `truetype` directly. A static face or a
    DejaVu fallback has no axes and keeps the weight it was built with, so the
    failure is ignored rather than reported.
    """
    from PIL import ImageFont

    font = ImageFont.truetype(path, size)
    weight = brand.weight_for(role)
    if weight:
        try:
            font.set_variation_by_name(weight)
        except Exception:  # noqa: BLE001 — a static face has no axes
            pass
    return font


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

    # The headline number is set in the accent serif when the brand face is
    # installed — the book's "accent headings" role — and in the bold sans
    # otherwise. Everything else is the sans, upper-case labels included.
    accent = _font_file(ACCENT_PATHS, "") or bold
    return _Fonts(
        label=_face(regular, 27, "body"),
        chip=_face(bold, 30, "heading"),
        # Not the accent serif, which carries no ₴ at all: Instrument Serif is
        # Latin and digits. The number beside this stays the serif — digits it
        # does have — and the mark is small and dimmer, so the two faces meet
        # where nobody reads them as one word.
        currency=_face(bold, 52, "heading"),
        stat=_face(bold, 50, "heading"),
        big=_face(accent, 124, "accent"),
    )


# ─── Drawing, with a fallback for the glyphs the brand faces lack ───────────
#
# Pillow has no font fallback: a missing glyph is drawn as an empty box and
# nothing says so. The brand faces carry everything this report writes except
# the two arrows, so text is drawn in runs and those characters come from
# DejaVu at the same size. When DejaVu *is* the primary face the run is the
# same file and the output is identical, which is why this is unconditional
# rather than a branch on which fonts happen to be installed.

_FALLBACK_FACES: dict = {}


def _fallback_face(size: int):
    from PIL import ImageFont

    if size not in _FALLBACK_FACES:
        path = _font_file(DEJAVU_PATHS, "DejaVuSans.ttf")
        _FALLBACK_FACES[size] = ImageFont.truetype(path, size) if path else None
    return _FALLBACK_FACES[size]


def _runs(text: str, font):
    """`text` split into (run, face) pairs, one face per contiguous stretch."""
    spare = _fallback_face(int(getattr(font, "size", 0) or 0))
    if spare is None or not any(c in brand.FALLBACK_CHARS for c in text):
        return [(text, font)]
    out, buf, buf_spare = [], "", False
    for ch in text:
        needs = ch in brand.FALLBACK_CHARS
        if buf and needs != buf_spare:
            out.append((buf, spare if buf_spare else font))
            buf = ""
        buf, buf_spare = buf + ch, needs
    if buf:
        out.append((buf, spare if buf_spare else font))
    return out


def _len(d, text: str, font) -> float:
    """`d.textlength`, measured per run so a fallback glyph counts."""
    return sum(d.textlength(run, font=face) for run, face in _runs(text, font))


def _text(d, xy, text: str, font=None, fill=None) -> None:
    """`d.text`, drawn per run so a missing glyph is never an empty box."""
    x, y = xy
    for run, face in _runs(text, font):
        d.text((x, y), run, font=face, fill=fill)
        x += d.textlength(run, font=face)


# ─── Small helpers ──────────────────────────────────────────────────────────

def _delta(pct: Optional[float], lang: str) -> Tuple[str, Tuple[int, int, int]]:
    """A change worth showing, or an honest mark that there is none."""
    if pct is None:
        return t("report.no_base", lang), MUTED
    if abs(pct) < 0.5:
        return t("report.flat", lang), MUTED
    return f"{'▲' if pct > 0 else '▼'} {abs(pct):.1f}%", UP if pct > 0 else DOWN


# ─── The card ───────────────────────────────────────────────────────────────

def _draw_header(d, report: WeeklyReport, fonts: _Fonts, lang: str) -> None:
    """Which week, and which book. The caption says the rest."""
    from core.weekly_report import _sales_type_label

    window = fmt_window(report.start, report.end, lang)
    label = _sales_type_label(report.sales_type, lang).upper()
    _text(d, (PAD, PAD), f"{window}   ·   {label}", font=fonts.label, fill=CARD_LABEL)


def _draw_headline(d, report: WeeklyReport, fonts: _Fonts, lang: str) -> None:
    """The number, and how it compares to the week before it."""
    cur, prev = report.current, report.previous

    # The currency mark is smaller and dimmer: at 124px the ₴ is as heavy as
    # a digit, and the eye reads "₴9" as one character.
    _text(d, (PAD, PAD + 104), "₴", font=fonts.currency, fill=CARD_LABEL)
    _text(d, (PAD + 56, PAD + 50), fmt_int(cur.revenue, lang),
           font=fonts.big, fill=CARD_TEXT)

    text, _ = _delta(pct_change(cur.revenue, prev.revenue if prev else None), lang)
    if prev is not None:
        text = f"{text} {t('report.vs_last_week', lang)}"
    # The chip is the brand's lime on bordeaux — its signature pairing — with
    # bordeaux text, whatever the sign; the arrow says which way.
    # Width from the run-aware measurement, because the arrow is a fallback
    # glyph and `textbbox` would size the chip for a face that lacks it.
    box = d.textbbox((0, 0), text, font=fonts.chip)
    w, h = _len(d, text, fonts.chip), box[3] - box[1]
    x, y = PAD, 270
    d.rounded_rectangle((x, y, x + w + 44, y + h + 32), radius=(h + 32) // 2,
                        fill=CHIP)
    _text(d, (x + 22, y + 14), text, font=fonts.chip, fill=CHIP_TEXT)


def _draw_stats(d, report: WeeklyReport, fonts: _Fonts, lang: str) -> None:
    """Orders, basket, and the split that says which of them moved."""
    cur, prev = report.current, report.previous
    d.line((PAD, STATS_TOP - 40, W - PAD, STATS_TOP - 40), fill=CARD_LINE, width=2)

    def cell(x: float, label: str, value: str,
             deltas: Sequence[Optional[float]]) -> None:
        _text(d, (x, STATS_TOP), label, font=fonts.label, fill=CARD_LABEL)
        _text(d, (x, STATS_TOP + 38), value, font=fonts.stat, fill=CARD_TEXT)
        # Two numbers in a cell need two deltas beside them; one would leave
        # the reader guessing which of them it belongs to.
        cursor = x
        for i, delta in enumerate(deltas):
            if i:
                _text(d, (cursor, STATS_TOP + 104), " / ", font=fonts.label, fill=CARD_LINE)
                cursor += _len(d, " / ", font=fonts.label)
            text, colour = _delta(delta, lang)
            if colour == MUTED:
                colour = CARD_LABEL
            _text(d, (cursor, STATS_TOP + 104), text, font=fonts.label, fill=colour)
            cursor += _len(d, text, font=fonts.label)

    width = (W - 2 * PAD) / 3
    cell(PAD, t("card.orders", lang), fmt_int(cur.orders, lang),
         [pct_change(cur.orders, prev.orders) if prev else None])
    cell(PAD + width, t("card.avg_check", lang), fmt_money(cur.avg_check, lang),
         [pct_change(cur.avg_check, prev.avg_check) if prev else None])

    if cur.new_customer_orders is not None:
        cell(PAD + 2 * width, t("card.new_repeat", lang),
             f"{fmt_int(cur.new_customer_orders, lang)} / "
             f"{fmt_int(cur.repeat_orders, lang)}",
             [pct_change(cur.new_customer_orders,
                         prev.new_customer_orders if prev else None),
              pct_change(cur.repeat_orders,
                         prev.repeat_orders if prev else None)])


def _tinted_mark(mask_path: str, colour, height: int):
    """A brand mask as an RGBA image in `colour`, scaled to `height`.

    None when the mask is not on this host: the card is complete without
    its marks, and a missing asset must not cost the picture.
    """
    from PIL import Image

    if not os.path.exists(mask_path):
        return None
    mask = Image.open(mask_path).convert("L")
    width = max(1, round(mask.width * height / mask.height))
    mask = mask.resize((width, height), Image.LANCZOS)
    mark = Image.new("RGBA", mask.size, (*colour, 0))
    mark.putalpha(mask)
    return mark


def _draw_marks(img, fonts: _Fonts) -> None:
    """The lime flower top right, the pink wordmark bottom right.

    The brand book's bordeaux pages (p. 2, 18, 24) carry exactly this pair;
    the card borrows the composition rather than inventing one.
    """
    flower = _tinted_mark(brand.FLOWER_MASK, brand.LIME, 96)
    if flower is not None:
        img.paste(flower, (W - PAD - flower.width, PAD - 8), flower)
    wordmark = _tinted_mark(brand.WORDMARK_MASK, brand.PINK, 40)
    if wordmark is not None:
        img.paste(wordmark, (W - PAD - wordmark.width, H - PAD + 14), wordmark)


def render_weekly_card(
    report: WeeklyReport, lang: str = DEFAULT_LANGUAGE,
) -> Optional[bytes]:
    """The card as PNG bytes in `lang`, or None if it could not be drawn.

    Never raises. A card is a nice-to-have wrapped around numbers that are not,
    so every failure here degrades to the text report rather than costing the
    week its message.
    """
    try:
        from PIL import Image, ImageDraw

        fonts = _load_fonts()
        if fonts is None:
            return None

        lang = normalize(lang)
        img = Image.new("RGB", (W, H), CARD_BG)
        d = ImageDraw.Draw(img)

        _draw_header(d, report, fonts, lang)
        _draw_headline(d, report, fonts, lang)
        _draw_stats(d, report, fonts, lang)
        _draw_marks(img, fonts)

        buf = io.BytesIO()
        img.save(buf, format="PNG", optimize=True)
        return buf.getvalue()
    except Exception as exc:
        logger.warning("Weekly report card failed to render: %s", exc, exc_info=True)
        return None


# ─── The two charts of the rich form ────────────────────────────────────────
#
# Same rules as the card: Pillow only, drawn at ~2× display size, every
# failure returns None and the report falls back to its tables. A chart says
# in one glance what a seven-row table says in seven reads, which is the
# whole reason the rich form carries them.

INK = brand.BORDEAUX      # this week
PALE = brand.PINK         # last week
STEP = brand.LIME         # the two effects in the waterfall: the accent
CONNECT = brand.BEIGE


@dataclass(frozen=True)
class _ChartFonts:
    label: object
    small: object
    value: object


def _load_chart_fonts() -> Optional[_ChartFonts]:
    regular = _font_file(FONT_PATHS, "DejaVuSans.ttf")
    bold = _font_file(BOLD_PATHS, "DejaVuSans-Bold.ttf")
    if not regular or not bold:
        logger.info("Weekly report charts skipped: DejaVu fonts not installed")
        return None
    # Through `_face`, not `truetype`: with the variable brand file both paths
    # are the same file, and calling `truetype` directly gave every chart's
    # bold label the default Regular instance instead.
    return _ChartFonts(
        label=_face(regular, 26, "body"),
        small=_face(regular, 22, "body"),
        value=_face(bold, 24, "heading"),
    )


def _centered(d, x: float, y: float, text: str, font, fill) -> None:
    w = _len(d, text, font=font)
    _text(d, (x - w / 2, y), text, font=font, fill=fill)


def render_days_chart(report: WeeklyReport, lang: str = DEFAULT_LANGUAGE) -> Optional[bytes]:
    """Seven grouped bars: this week in ink, the same weekday last week pale.

    Values sit above this week's bars, order counts under the weekday. The
    caption in the message says which week is which; the legend repeats it
    for a reader who saved the picture alone.
    """
    try:
        from PIL import Image, ImageDraw

        if not report.days:
            return None
        fonts = _load_chart_fonts()
        if fonts is None:
            return None
        lang = normalize(lang)

        w, h = 1200, 600
        img = Image.new("RGB", (w, h), BG)
        d = ImageDraw.Draw(img)

        prev = {x.day.weekday(): x for x in report.previous_days}
        values = [x.revenue for x in report.days]
        before = [prev[x.day.weekday()].revenue if x.day.weekday() in prev else 0.0
                  for x in report.days]
        top = max(values + before) or 1.0

        # Legend, top right.
        lx, ly = w - PAD - 20, PAD - 10
        for label, colour in ((t("report.col_last_week", lang), PALE),
                              (t("report.col_this_week", lang), INK)):
            tw = _len(d, label, font=fonts.small)
            _text(d, (lx - tw, ly), label, font=fonts.small, fill=MUTED)
            d.rounded_rectangle((lx - tw - 34, ly + 3, lx - tw - 12, ly + 21), radius=4, fill=colour)
            lx = lx - tw - 60

        plot_top, plot_bottom = 130, 470
        plot_left, plot_right = PAD, w - PAD
        d.line((plot_left, plot_bottom, plot_right, plot_bottom), fill=LINE, width=2)

        n = len(report.days)
        group = (plot_right - plot_left) / n
        bar = group * 0.30
        for i, day in enumerate(report.days):
            cx = plot_left + group * (i + 0.5)
            for offset, value, colour in ((-bar / 2 - 3, before[i], PALE), (bar / 2 + 3, values[i], INK)):
                height = (plot_bottom - plot_top) * value / top
                x0 = cx + offset - bar / 2
                if value > 0:
                    d.rounded_rectangle((x0, plot_bottom - height, x0 + bar, plot_bottom),
                                        radius=6, fill=colour)
            # The value sits on its own bar — centred over this week's, just
            # above its top — on a small canvas-coloured pill, so it stays
            # legible where last week's taller bar runs behind it.
            height = (plot_bottom - plot_top) * values[i] / top
            label = compact_money(values[i], lang)
            lx = cx + bar / 2 + 3
            ly = plot_bottom - height - 34
            lw = _len(d, label, font=fonts.value)
            d.rounded_rectangle((lx - lw / 2 - 8, ly - 4, lx + lw / 2 + 8, ly + 28),
                                radius=8, fill=BG)
            _centered(d, lx, ly, label, fonts.value, TEXT)
            _centered(d, cx, plot_bottom + 14, t(f"weekday.{day.day.weekday()}", lang),
                      fonts.label, TEXT)
            _centered(d, cx, plot_bottom + 48, fmt_int(day.orders, lang), fonts.small, MUTED)

        buf = io.BytesIO()
        img.save(buf, format="PNG", optimize=True)
        return buf.getvalue()
    except Exception as exc:
        logger.warning("Days chart failed to render: %s", exc, exc_info=True)
        return None


def render_waterfall(report: WeeklyReport, lang: str = DEFAULT_LANGUAGE) -> Optional[bytes]:
    """Last week → the order effect → the basket effect → this week.

    The decomposition is exact, so the four bars close: the two floating
    steps land precisely on this week's total, and a reader sees which of
    the two carried the move without reading a percentage.
    """
    try:
        from PIL import Image, ImageDraw

        cur, prev = report.current, report.previous
        if prev is None:
            return None
        fonts = _load_chart_fonts()
        if fonts is None:
            return None
        lang = normalize(lang)

        orders_effect, check_effect = decompose(cur, prev)
        steps = [
            (t("report.col_last_week", lang), 0.0, prev.revenue, PALE),
            (t("report.effect_orders_down" if orders_effect < 0 else "report.effect_orders_up", lang),
             prev.revenue, prev.revenue + orders_effect, STEP),
            (t("report.effect_check_down" if check_effect < 0 else "report.effect_check_up", lang),
             prev.revenue + orders_effect, cur.revenue, STEP),
            (t("report.col_this_week", lang), 0.0, cur.revenue, INK),
        ]
        labels = [prev.revenue, orders_effect, check_effect, cur.revenue]

        w, h = 1200, 560
        img = Image.new("RGB", (w, h), BG)
        d = ImageDraw.Draw(img)

        top = max(prev.revenue, cur.revenue, prev.revenue + orders_effect) * 1.12 or 1.0
        plot_top, plot_bottom = 90, 430
        plot_left, plot_right = PAD, w - PAD
        d.line((plot_left, plot_bottom, plot_right, plot_bottom), fill=LINE, width=2)

        group = (plot_right - plot_left) / len(steps)
        bar = group * 0.5

        def y_of(v: float) -> float:
            return plot_bottom - (plot_bottom - plot_top) * max(v, 0.0) / top

        prev_x_right = None
        prev_y = None
        for i, ((label, start, end, colour), value) in enumerate(zip(steps, labels)):
            cx = plot_left + group * (i + 0.5)
            x0, x1 = cx - bar / 2, cx + bar / 2
            y0, y1 = sorted((y_of(start), y_of(end)))
            d.rounded_rectangle((x0, y0, x1, max(y1, y0 + 4)), radius=6, fill=colour)
            if prev_x_right is not None:
                d.line((prev_x_right, prev_y, x0, prev_y), fill=CONNECT, width=2)
            prev_x_right, prev_y = x1, y_of(end)
            text = compact_money(value, lang) if i in (0, 3) else (
                f"{'+' if value >= 0 else '-'}{compact_money(abs(value), lang)}")
            # Labels in ink throughout: lime text on the off-white canvas is
            # the one pairing the brand book rules out (p. 14).
            _centered(d, cx, y0 - 34, text, fonts.value, INK if i in (1, 2) else TEXT)
            _centered(d, cx, plot_bottom + 16, label, fonts.label, TEXT)

        buf = io.BytesIO()
        img.save(buf, format="PNG", optimize=True)
        return buf.getvalue()
    except Exception as exc:
        logger.warning("Waterfall chart failed to render: %s", exc, exc_info=True)
        return None


# One brand colour per channel, in the order the report lists them (largest
# first). Lime and pink are fills on the off-white canvas, which p. 14 allows
# for graphics; the labels beside them stay in ink.
CHANNEL_COLOURS = (brand.BORDEAUX, brand.LIME, brand.PINK, brand.PLUM, brand.BEIGE)


def render_channels_chart(report: WeeklyReport, lang: str = DEFAULT_LANGUAGE) -> Optional[bytes]:
    """One row per channel: this week as a filled bar, last week as an outline.

    Length is revenue on a shared scale, so the rows compare with each other
    and each with its own last week. The share and the change are written
    out beside the bar — a picture of a proportion and the number it stands
    for, which is what the block-glyph bars in the table could not be.
    """
    try:
        from PIL import Image, ImageDraw

        if not report.channels:
            return None
        fonts = _load_chart_fonts()
        if fonts is None:
            return None
        lang = normalize(lang)

        rows = report.channels[:len(CHANNEL_COLOURS)]
        row_h = 118
        w, h = 1200, PAD + row_h * len(rows) + 24
        img = Image.new("RGB", (w, h), BG)
        d = ImageDraw.Draw(img)

        total = sum(c.revenue for c in rows) or 1.0
        top = max([c.revenue for c in rows] + [c.previous_revenue for c in rows]) or 1.0
        label_w = 260
        bar_left, bar_right = PAD + label_w, w - PAD
        # The longest bar stops short of the edge so its label still fits.
        span = bar_right - bar_left - 230

        for i, c in enumerate(rows):
            y = PAD + row_h * i
            colour = CHANNEL_COLOURS[i]
            # Name, then the change under it, in the label column.
            _text(d, (PAD, y), c.name, font=fonts.value, fill=TEXT)
            # Only the change under the name: the outline below the bar is
            # last week, and the legend says so once.
            change, _ = _delta(pct_change(c.revenue, c.previous_revenue), lang)
            _text(d, (PAD, y + 34), change, font=fonts.small, fill=MUTED)
            # This week: a filled bar with the revenue and share at its end.
            length = span * c.revenue / top
            d.rounded_rectangle((bar_left, y, bar_left + max(length, 6), y + 36),
                                radius=8, fill=colour)
            _text(d, (bar_left + length + 14, y + 4),
                   f"{compact_money(c.revenue, lang)} · {c.revenue / total * 100:.0f}%",
                   font=fonts.value, fill=TEXT)
            # Last week: the same scale, drawn as an outline underneath.
            prev_len = span * c.previous_revenue / top
            if prev_len > 0:
                d.rounded_rectangle((bar_left, y + 46, bar_left + max(prev_len, 6), y + 66),
                                    radius=6, outline=brand.PLUM, width=2)
                _text(d, (bar_left + prev_len + 14, y + 42),
                       compact_money(c.previous_revenue, lang), font=fonts.small, fill=MUTED)

        # Legend for the outline, once, bottom left.
        ly = PAD + row_h * len(rows) - 14
        d.rounded_rectangle((PAD, ly + 6, PAD + 34, ly + 20), radius=4, outline=brand.PLUM, width=2)
        _text(d, (PAD + 46, ly), t("report.col_last_week", lang), font=fonts.small, fill=MUTED)

        buf = io.BytesIO()
        img.save(buf, format="PNG", optimize=True)
        return buf.getvalue()
    except Exception as exc:
        logger.warning("Channels chart failed to render: %s", exc, exc_info=True)
        return None
