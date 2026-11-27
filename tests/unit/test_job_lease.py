"""K0's switch and lease key, as the other process must compute them.

The Rust worker reads the same switch and takes the same advisory key, so
what is pinned here is a contract between two implementations, not a
constant: the vectors below are the ones the pull request gives the Rust
side. The behaviour over Postgres is `tests/integration/test_job_lease_pg.py`.
"""
from __future__ import annotations

import asyncio

import pytest

from core import job_lease

# The contract's test vectors: job id -> its advisory key. A change to the
# formula moves these, and the Rust side's copy with them.
CONTRACT_VECTORS = {
    "ch_sync": 7742613876423907721,
    "incremental_sync": 7742613875706779467,
    "memory_monitor": 7742613878325278635,
}


class TestTheLeaseKey:
    @pytest.mark.parametrize("job_id, key", sorted(CONTRACT_VECTORS.items()))
    def test_the_key_is_the_contracts(self, job_id, key):
        assert job_lease.lease_key(job_id) == key

    def test_every_job_has_a_key_of_its_own_in_the_jobs_prefix(self):
        """A collision would make two jobs exclude each other across
        processes; a key outside the prefix could meet another advisory
        lock. Every key is also a positive bigint, the same number on both
        sides of the wire."""
        from core.scheduler import registered_job_ids

        jobs = asyncio.run(registered_job_ids())
        keys = {job_lease.lease_key(j) for j in jobs}
        assert len(keys) == len(jobs)
        assert all(k >> 32 == job_lease.LEASE_PREFIX for k in keys)
        assert all(0 < k < 2 ** 63 for k in keys)

    def test_a_name_with_a_colon_is_left_to_the_rust_side(self):
        """The contract reserves names with a `:` for the Rust worker's own
        leases (`ch_sync:shadow` for a shadow run), so no job may carry one,
        and the shadow's key is not a job's."""
        from core.scheduler import registered_job_ids

        jobs = asyncio.run(registered_job_ids())
        assert not [j for j in jobs if ":" in j]
        assert job_lease.lease_key("ch_sync:shadow") not in {
            job_lease.lease_key(j) for j in jobs}


class TestTheSwitch:
    def test_exactly_rust_hands_a_job_to_rust(self):
        assert job_lease.executor_of("rust") == job_lease.RUST

    @pytest.mark.parametrize("value", [None, "python", "Rust", "RUST", " rust",
                                       "rust ", "", "rust\n", "ruѕt"])
    def test_anything_else_keeps_it_in_python(self, value):
        """No trimming and no case folding: two languages trim different
        whitespace, and a value both sides could read differently would be
        a job both think is theirs. The last one has a Cyrillic `ѕ`."""
        assert job_lease.executor_of(value) == job_lease.PYTHON

    def test_the_switch_is_a_row_of_the_jobs_own_name(self):
        assert job_lease.switch_key("ch_sync") == "executor:ch_sync"


class TestTheKillSwitch:
    @pytest.mark.parametrize("value", ["off", "OFF", "0", "false", " off "])
    def test_off_turns_the_leases_off(self, monkeypatch, value):
        monkeypatch.setenv(job_lease.LEASES_ENV, value)
        assert job_lease.leases_on() is False
        assert job_lease.from_environment() is None

    @pytest.mark.parametrize("value", ["", "on", "1", "true", "of", "yes"])
    def test_anything_else_keeps_them_on(self, monkeypatch, value):
        """A typo must not let a switched job back into Python."""
        monkeypatch.setenv(job_lease.LEASES_ENV, value)
        assert job_lease.leases_on() is True

    def test_without_postgres_there_is_nothing_to_lease_on(self, monkeypatch):
        monkeypatch.delenv(job_lease.LEASES_ENV, raising=False)
        monkeypatch.delenv("KS_PG_DSN", raising=False)
        assert job_lease.from_environment() is None
