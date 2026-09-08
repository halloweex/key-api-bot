"""Korean Story's visual identity, as the brand book states it.

Source: "Brand Identity & Guidelines" (BLUFF studio), p. 13 colours, p. 14
colour-on-background, p. 15 and 46 typography, p. 17 alignment. The values
are copied from the page, not eyeballed; when the book changes, this file
changes with it and nothing else has to.

Only pictures can carry any of this — Telegram renders message text in its
own face and colours — so the report's card and charts are the consumers.
"""
from __future__ import annotations

import os
from typing import Optional, Sequence, Tuple

RGB = Tuple[int, int, int]

# ─── Colours (p. 13) ────────────────────────────────────────────────────────
#
# Four primary, two additional. The additional ones are for text and graphic
# elements in content, which is exactly what a chart is.

BORDEAUX: RGB = (130, 21, 50)      # #821532 · Pantone 7637 C — the brand colour
LIME: RGB = (220, 223, 93)         # #dcdf5d · Pantone 2296 C — the accent
OFF_WHITE: RGB = (245, 244, 235)   # #f5f4eb — the canvas
PINK: RGB = (247, 201, 223)        # #f7c9df · Pantone 217 C
BEIGE: RGB = (227, 212, 210)       # #e3d4d2 · Pantone 7604 C — additional
PLUM: RGB = (146, 33, 70)          # #922146 · Pantone 676 C — additional

# Body text in the book itself is a near-black on off-white; neither is a
# brand colour, both are what the palette is set against. Kept here so the
# pictures agree with each other.
TEXT: RGB = (43, 35, 38)
MUTED: RGB = (140, 123, 128)

# ─── Which colour on which background (p. 14) ───────────────────────────────
#
# Text must keep full contrast; graphics may use the soft pairs. On off-white
# the bordeaux family and lime read; on bordeaux, lime / pink / beige /
# off-white read. Lime text on off-white does not, so lime is a fill and a
# chip, never a label on the light canvas.

# ─── Typography (p. 15, 46) ─────────────────────────────────────────────────
#
# Libre Franklin for headings, subheadings and body: Bold for headings and
# accents, Medium for body; headings in upper case. Instrument Serif Regular
# for accent headings, every word capitalised. Alignment left or centre,
# never right or justified (p. 17) — chart value labels are centred over
# their bars for that reason.

_FONT_DIRS = (
    os.path.join(os.path.dirname(os.path.dirname(__file__)), "assets", "fonts"),
    "/usr/share/fonts/truetype/korean-story",
)


def _first_existing(names: Sequence[str]) -> Optional[str]:
    for directory in _FONT_DIRS:
        for name in names:
            path = os.path.join(directory, name)
            if os.path.exists(path):
                return path
    return None


def brand_font(role: str) -> Optional[str]:
    """Path to the brand face for `role`, or None when it is not installed.

    Roles: `body` (Libre Franklin Medium, Regular as a stand-in), `heading`
    (Libre Franklin Bold), `accent` (Instrument Serif Regular). The renderers
    fall back to DejaVu — which carries Cyrillic and ₴ — so a host without
    the brand fonts still draws the picture, just not in the brand's hand.
    """
    return _first_existing({
        "body": ("LibreFranklin[wght].ttf", "LibreFranklin-Medium.ttf",
                 "LibreFranklin-Regular.ttf"),
        "heading": ("LibreFranklin[wght].ttf", "LibreFranklin-Bold.ttf",
                    "LibreFranklin-SemiBold.ttf"),
        "accent": ("InstrumentSerif-Regular.ttf",),
    }[role])


# Google Fonts ships Libre Franklin as one variable file, so a weight is an
# axis setting and not a second file: `body` and `heading` resolve to the same
# path and are told apart by this. The renderer applies it after loading and
# ignores the failure, so a static face that already is that weight, or a
# DejaVu fallback with no axes, still draws.
_WEIGHTS = {"body": "Medium", "heading": "Bold"}


def weight_for(role: str) -> Optional[str]:
    """The variable-font instance `role` should be set to, if any."""
    return _WEIGHTS.get(role)


# What the brand faces do not carry, measured rather than assumed:
# Libre Franklin covers Latin, Cyrillic, digits and ₴ but not the geometric
# arrows the deltas are written with, and Instrument Serif is Latin and digits
# alone — no Cyrillic, no ₴. So the arrows are drawn from the fallback face at
# the same size, and the ₴ is set in Libre Franklin even where the number
# beside it is the accent serif. `tests/unit/test_brand_fonts.py` computes
# this set from the bundled files and fails if it drifts.
FALLBACK_CHARS = "▲▼"


# ─── The marks ──────────────────────────────────────────────────────────────
#
# The flower sign and the wordmark, as greyscale masks lifted off the
# identity pages (the shop bot's `webapp/` had them first; copied, not
# linked, so this image builds on its own). White is ink: tint the mask with
# a palette colour and it becomes the mark in that colour.

_BRAND_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), "assets", "brand")
FLOWER_MASK = os.path.join(_BRAND_DIR, "flower.png")
WORDMARK_MASK = os.path.join(_BRAND_DIR, "wordmark.png")
