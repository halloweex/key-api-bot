"""The brand faces draw the report, and neither of them covers everything.

Libre Franklin carries Latin, Cyrillic, digits and ₴ but not the geometric
arrows the deltas are written with. Instrument Serif is Latin and digits
alone — no Cyrillic, no ₴ — which is fine for the one thing it sets, the
headline number, and would be a row of boxes anywhere else.

Pillow has no font fallback and draws a missing glyph as `.notdef`, silently.
So the gap is measured here rather than assumed, from the files that actually
ship, and `brand.FALLBACK_CHARS` is asserted to be exactly that gap: too small
and the report grows empty boxes, too large and characters are routed to a
face nobody asked for.

`.notdef` is detected by rendering a private-use codepoint no font maps and
comparing masks — fontTools would be cleaner and is not in the CI lock.
"""
from pathlib import Path

import pytest

from core import brand
from core.i18n import LANGUAGES, fmt_int, fmt_money, fmt_window, t

ROOT = Path(__file__).resolve().parents[2]
UNMAPPED = ""  # private use area: no real font claims it


def _renders(font):
    """A predicate saying whether `font` has a real glyph for a character."""
    notdef = bytes(font.getmask(UNMAPPED))

    def has(ch: str) -> bool:
        return bytes(font.getmask(ch)) != notdef

    return has


def _face(path, size=40):
    from PIL import ImageFont

    return ImageFont.truetype(str(path), size)


# Every character the *pictures* can draw. Not the whole translation table:
# the message text carries emoji and product names, and neither reaches a
# canvas. Assembled from the real formatters so a new label lands here on its
# own rather than being remembered.
def _drawable_alphabet() -> set:
    from datetime import date

    chars = set("0123456789 ")
    for lang in LANGUAGES:
        for key in (
            "report.vs_last_week", "report.col_last_week", "report.col_this_week",
            "report.no_base", "report.flat",
            "report.effect_orders_down", "report.effect_orders_up",
            "report.effect_check_down", "report.effect_check_up",
            "card.orders", "card.avg_check", "card.new_repeat",
            *(f"weekday.{n}" for n in range(7)),
            *(f"report.sales_type.{n}"
              for n in ("retail", "b2b", "internal", "exhibition", "all")),
        ):
            chars |= set(t(key, lang))
            chars |= set(t(key, lang).upper())
        chars |= set(fmt_money(1_234_567.89, lang))
        chars |= set(fmt_int(1_234_567, lang))
        chars |= set(fmt_window(date(2026, 8, 31), date(2026, 9, 6), lang))
    # Rendered by hand rather than through a translation: the delta arrows,
    # the separators the layouts write, and the compact magnitudes.
    chars |= set("▲▼+-−%·/₴KM.,:—–")
    # Channel names come from the warehouse, not the table, and are Latin.
    chars |= set("InstagramShopifyTelegram")
    return chars


class TestTheFilesShip:
    def test_both_faces_are_in_the_repository(self):
        assert brand.brand_font("body"), "Libre Franklin is not in assets/fonts"
        assert brand.brand_font("accent"), "Instrument Serif is not in assets/fonts"

    def test_body_and_heading_are_one_variable_file(self):
        """Google Fonts ships Libre Franklin as a single variable file, which
        is why the weight is an axis and not a second path."""
        assert brand.brand_font("body") == brand.brand_font("heading")
        assert brand.weight_for("body") == "Medium"
        assert brand.weight_for("heading") == "Bold"
        assert brand.weight_for("accent") is None

    def test_the_licences_travel_with_the_fonts(self):
        """OFL 1.1 requires it, and a font without its licence is a licence
        violation sitting in a public repository."""
        for name in ("LibreFranklin-OFL.txt", "InstrumentSerif-OFL.txt"):
            licence = ROOT / "assets" / "fonts" / name
            assert licence.exists(), name
            assert "SIL OPEN FONT LICENSE" in licence.read_text().upper()


class TestCoverage:
    def test_the_fallback_set_is_exactly_what_libre_franklin_lacks(self):
        """The one assertion this file exists for. Computed from the shipped
        file over the alphabet the pictures can draw."""
        has = _renders(_face(brand.brand_font("body")))
        missing = {c for c in _drawable_alphabet() if not has(c)}
        assert missing == set(brand.FALLBACK_CHARS), {
            "missing but not routed": sorted(missing - set(brand.FALLBACK_CHARS)),
            "routed but not missing": sorted(set(brand.FALLBACK_CHARS) - missing),
        }

    def test_the_accent_serif_covers_the_one_thing_it_sets(self):
        """`big` draws the headline number and nothing else — no Cyrillic and
        no ₴, both of which this face lacks."""
        has = _renders(_face(brand.brand_font("accent")))
        for lang in LANGUAGES:
            for ch in fmt_int(1_234_567, lang):
                assert has(ch), f"{ch!r} of a {lang} number is not in the serif"

    def test_the_accent_serif_would_not_do_for_anything_else(self):
        """Pinned so nobody widens its role by eye: this is *why* the ₴ beside
        the headline is set in Libre Franklin."""
        has = _renders(_face(brand.brand_font("accent")))
        assert not has("₴")
        assert not has("Ж")

    def test_the_fallback_face_covers_what_the_brand_faces_do_not(self):
        from core.weekly_report_image import _fallback_face

        spare = _fallback_face(40)
        if spare is None:
            pytest.skip("no DejaVu on this host")
        has = _renders(spare)
        for ch in brand.FALLBACK_CHARS:
            assert has(ch), f"the fallback face cannot draw {ch!r} either"


class TestRunSplitting:
    def test_every_character_lands_in_a_face_that_can_draw_it(self):
        from core.weekly_report_image import _load_fonts, _runs

        fonts = _load_fonts()
        if fonts is None:
            pytest.skip("no fonts on this host")
        text = "▼ 28.1% против прошлой недели ▲ ₴ 805 788"
        for run, face in _runs(text, fonts.label):
            has = _renders(face)
            for ch in run:
                assert has(ch), f"{ch!r} was routed to a face without it"

    def test_a_string_without_arrows_is_not_split(self):
        """The split costs a draw call per run; text that does not need it
        must not pay."""
        from core.weekly_report_image import _load_fonts, _runs

        fonts = _load_fonts()
        if fonts is None:
            pytest.skip("no fonts on this host")
        assert _runs("Выручка ₴ 805 788", fonts.label) == [
            ("Выручка ₴ 805 788", fonts.label)
        ]

    def test_the_runs_reassemble_into_the_original(self):
        from core.weekly_report_image import _load_fonts, _runs

        fonts = _load_fonts()
        if fonts is None:
            pytest.skip("no fonts on this host")
        for text in ("▼ 22.4%", "▲", "", "abc", "a▼b▲c", "▼▲"):
            assert "".join(run for run, _ in _runs(text, fonts.label)) == text


class TestTheWeightAxisIsApplied:
    def test_heading_is_heavier_than_body(self):
        """Without `set_variation_by_name` the variable file loads at Regular
        and every bold label on a card or chart is quietly not bold."""
        from PIL import Image, ImageDraw

        from core.weekly_report_image import _face

        path = brand.brand_font("body")
        if not path:
            pytest.skip("brand fonts not installed")
        d = ImageDraw.Draw(Image.new("RGB", (10, 10)))
        body = d.textlength("Выручка 805 788", font=_face(path, 40, "body"))
        heading = d.textlength("Выручка 805 788", font=_face(path, 40, "heading"))
        assert heading > body, (body, heading)

    def test_a_face_without_axes_is_not_an_error(self):
        """The DejaVu fallback and the static serif have no axes; asking for
        a weight there must be ignored, not raised."""
        from core.weekly_report_image import _face

        assert _face(brand.brand_font("accent"), 40, "heading") is not None
