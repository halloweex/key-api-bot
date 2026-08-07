"""The DuckDB memory ceiling must be tunable without a code deploy.

3GB hardcoded is what OOM'd seven refreshes in a row on 2026-08-02 while the
container still had ~6GB of its budget free.
"""
import pytest

from core.duckdb_store import DEFAULT_DUCKDB_MEMORY_LIMIT, _memory_limit


class TestMemoryLimitResolution:
    def test_defaults_when_unset(self, monkeypatch):
        monkeypatch.delenv("DUCKDB_MEMORY_LIMIT", raising=False)
        assert _memory_limit() == DEFAULT_DUCKDB_MEMORY_LIMIT

    def test_default_is_above_the_ceiling_that_failed(self):
        assert DEFAULT_DUCKDB_MEMORY_LIMIT != "3GB"
        assert int(DEFAULT_DUCKDB_MEMORY_LIMIT.rstrip("GB")) > 3

    @pytest.mark.parametrize("value", ["5GB", "512MB", "2.5GB", "6GiB", "4gb", " 4GB "])
    def test_accepts_duckdb_size_syntax(self, monkeypatch, value):
        monkeypatch.setenv("DUCKDB_MEMORY_LIMIT", value)
        assert _memory_limit() == value.strip()

    @pytest.mark.parametrize("value", [
        "4",                      # no unit — DuckDB reads bare digits as bytes
        "lots",
        "4GB; DROP TABLE orders",  # interpolated straight into SET
        "-4GB",
    ])
    def test_rejects_anything_else(self, monkeypatch, value):
        monkeypatch.setenv("DUCKDB_MEMORY_LIMIT", value)
        assert _memory_limit() == DEFAULT_DUCKDB_MEMORY_LIMIT

    def test_blank_is_treated_as_unset(self, monkeypatch):
        monkeypatch.setenv("DUCKDB_MEMORY_LIMIT", "   ")
        assert _memory_limit() == DEFAULT_DUCKDB_MEMORY_LIMIT
