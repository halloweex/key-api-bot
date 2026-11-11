"""A text cell a customer typed, made safe for a spreadsheet to open.

A CSV cell that starts with ``=``, ``+``, ``-`` or ``@`` — or a tab or a
carriage return, which some spreadsheets skip before reading the next
character — is read by Excel, LibreOffice and Google Sheets as a formula, not
as text. The SMS roster carries names, cities and product names exactly as
customers typed them into the shop (KeyCRM buyers and order lines), so a buyer
named ``=HYPERLINK("…","x")`` became a live link in the file a marketer opens
(L3-07). The usual guard, OWASP's: a leading apostrophe, which the
spreadsheet shows as nothing and reads as "this is text".

Only for text that came from somebody else. A number or a phone number this
code wrote itself (``+380…``, the gateway's E.164) is never passed here: the
apostrophe would be a character the gateway reads.
"""
from __future__ import annotations

from typing import Any

_FORMULA_LEADS = ("=", "+", "-", "@", "\t", "\r")


def text_cell(value: Any) -> Any:
    """`value` as a CSV cell no spreadsheet reads as a formula. Non-text and
    empty values pass through unchanged."""
    if isinstance(value, str) and value.startswith(_FORMULA_LEADS):
        return "'" + value
    return value
