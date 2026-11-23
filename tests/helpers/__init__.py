"""Helpers that more than one test module uses.

A test module never imports another test module: deleting one would break
the other. What two of them share lives here (or in `tests/conftest.py`,
for a fixture requested by name across the suite).
"""
