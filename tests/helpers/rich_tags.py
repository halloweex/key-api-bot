"""The Bot API rich-form tags the reports may use."""
from __future__ import annotations


# Every tag the Bot API lists under "Rich HTML style" that this report may
# use. Anything outside the list is a 400 from Telegram, and the message
# falls back to the caption form — so a new tag here needs the docs first.
SUPPORTED_TAGS = {
    "h1", "h2", "h3", "p", "b", "i", "u", "s", "br", "ul", "ol", "li",
    "table", "tr", "th", "td", "figure", "img", "figcaption", "details",
    "summary", "blockquote", "footer", "hr", "a", "code", "pre",
    "tg-button", "tg-button-row", "tg-reference", "tg-math", "tg-math-block",
}
