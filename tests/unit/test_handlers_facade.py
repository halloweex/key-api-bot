"""`bot/main.py` reaches its handlers through the facade, so the facade decides.

`bot/handlers/__init__.py` re-exports `bot/handlers_legacy.py` and adds nothing,
which makes it easy to forget: a handler written in the legacy module works
under every test that imports that module directly, and does not exist at all
for `bot/main.py`, which says `handlers.<name>`.

That is not theoretical. On 2026-09-11 `alert_action_callback` shipped without
its re-export and the bot crash-looped in production on

    AttributeError: module 'bot.handlers' has no attribute 'alert_action_callback'

— after a green suite, because every test for it imported `bot.handlers_legacy`.
The registration is the only place the two names have to agree, so this reads
the registration.
"""
import ast
import re
from pathlib import Path

import pytest

import bot.handlers as facade

MAIN = Path(__file__).resolve().parents[2] / "bot" / "main.py"


def _referenced_names() -> set:
    """Every `handlers.<name>` `bot/main.py` mentions.

    Parsed rather than grepped: a comment or a docstring naming a handler is
    not a reference, and a test that fails on prose is a test somebody deletes.
    """
    tree = ast.parse(MAIN.read_text())
    names = set()
    for node in ast.walk(tree):
        if (isinstance(node, ast.Attribute)
                and isinstance(node.value, ast.Name)
                and node.value.id == "handlers"):
            names.add(node.attr)
    return names


def test_main_references_something_at_all():
    """A guard that found nothing would pass forever."""
    assert len(_referenced_names()) > 20


@pytest.mark.parametrize("name", sorted(_referenced_names()))
def test_every_handler_main_registers_exists_on_the_facade(name):
    assert hasattr(facade, name), (
        f"bot/main.py registers handlers.{name}, and the facade does not "
        f"export it. Add it to the import list *and* __all__ in "
        f"bot/handlers/__init__.py — the bot fails at startup, not at import, "
        f"so nothing else catches this."
    )


def test_the_facade_exports_what_it_declares():
    """`__all__` and the import list are two lists of the same thing, and a
    name in one only is the same bug wearing a different hat."""
    missing = [n for n in getattr(facade, "__all__", []) if not hasattr(facade, n)]
    assert not missing, f"__all__ names what is not imported: {missing}"
