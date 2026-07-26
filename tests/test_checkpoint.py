"""Tests for Checkpoint — completion tracking keyed by input."""

import pytest

from osiiso import AsyncQueue, Checkpoint, ExecutionError, ProcessQueue, ThreadQueue, amap, pmap, tmap
from osiiso.checkpoint import canonical_key

# Module-level callables so ProcessQueue can pickle them.


def double(x):
    return x * 2


def boom_on_odd(x):
    """Succeed on even inputs, fail on odd ones — a stand-in for a partial crash."""
    if x % 2:
        raise ValueError(f"odd: {x}")
    return x * 10


def never_call(x):
    raise AssertionError(f"{x} should have been restored from the checkpoint")


def add(a, b):
    return a + b


@pytest.fixture
def store(tmp_path):
    """A file-backed checkpoint that is closed at teardown."""
    with Checkpoint(tmp_path / "run.sqlite") as cp:
        yield cp


class TestCanonicalKey:
    def test_stable_across_calls(self):
        assert canonical_key({"b": 1, "a": 2}) == canonical_key({"a": 2, "b": 1})

    def test_distinguishes_types(self):
        assert canonical_key(1) != canonical_key("1")

    def test_tuple_and_list_agree(self):
        # _fan treats both as positional args, so they must key the same.
        assert canonical_key([1, 2]) == canonical_key((1, 2))

    def test_rejects_non_json(self):
        with pytest.raises(TypeError, match="pass key="):
            canonical_key(object())


class TestStore:
    def test_record_and_is_done(self, store):
        assert not store.is_done("ns", "a")
        assert store.record("ns", "a", 1)
        assert store.is_done("ns", "a")

    def test_lookup_returns_values(self, store):
        store.record("ns", "a", {"v": 1})
        hits = store.lookup("ns", ["a", "b"])
        assert hits["a"].value == {"v": 1}
        assert hits["a"].has_value
        assert "b" not in hits

    def test_lookup_batches_beyond_sqlite_limit(self, store):
        keys = [str(i) for i in range(1200)]
        for k in keys:
            store.record("ns", k, int(k))
        hits = store.lookup("ns", keys)
        assert len(hits) == 1200
        assert hits["999"].value == 999

    def test_namespaces_are_isolated(self, store):
        store.record("one", "a", 1)
        assert store.is_done("one", "a")
        assert not store.is_done("two", "a")
        assert store.namespaces() == ("one",)

    def test_count_and_clear(self, store):
        store.record("one", "a", 1)
        store.record("two", "b", 2)
        assert store.count() == 2
        assert store.count("one") == 1
        assert store.clear("one") == 1
        assert store.count() == 1
        assert store.clear() == 1
        assert store.count() == 0

    def test_record_is_idempotent(self, store):
        store.record("ns", "a", 1)
        store.record("ns", "a", 2)
        assert store.count("ns") == 1
        assert store.lookup("ns", ["a"])["a"].value == 2

    def test_unencodable_value_is_not_recorded(self, store, caplog):
        assert store.record("ns", "a", object()) is False
        assert not store.is_done("ns", "a")
        assert "not encodable" in caplog.text

    def test_store_values_false_records_key_only(self, tmp_path):
        with Checkpoint(tmp_path / "c.sqlite", store_values=False) as cp:
            assert cp.record("ns", "a", object())  # unencodable value is fine here
            hit = cp.lookup("ns", ["a"])["a"]
            assert hit.value is None
            assert hit.has_value is False

    def test_custom_encoder_and_decoder(self, tmp_path):
        with Checkpoint(tmp_path / "c.sqlite", encoder=lambda v: f"<{v}>", decoder=lambda b: str(b).strip("<>")) as cp:
            cp.record("ns", "a", "hi")
            assert cp.lookup("ns", ["a"])["a"].value == "hi"

    def test_undecodable_row_is_skipped(self, tmp_path):
        path = tmp_path / "c.sqlite"
        with Checkpoint(path, encoder=lambda v: "}}not json{{") as cp:
            cp.record("ns", "a", 1)
        with Checkpoint(path) as cp:  # default JSON decoder cannot read it back
            assert cp.lookup("ns", ["a"]) == {}

    def test_persists_across_open(self, tmp_path):
        path = tmp_path / "c.sqlite"
        with Checkpoint(path) as cp:
            cp.record("ns", "a", [1, 2])
        with Checkpoint(path) as cp:
            assert cp.lookup("ns", ["a"])["a"].value == [1, 2]

    def test_use_after_close_raises(self, tmp_path):
        cp = Checkpoint(tmp_path / "c.sqlite")
        cp.close()
        cp.close()  # idempotent
        with pytest.raises(RuntimeError, match="closed"):
            cp.record("ns", "a", 1)

    def test_repr(self, store):
        store.record("ns", "a", 1)
        assert "entries=1" in repr(store)


class TestResume:
    def test_second_run_skips_completed(self, store):
        with ThreadQueue(workers=4) as q:
            grp = q.group(boom_on_odd, [1, 2, 3, 4], checkpoint=store)
            q.run()
        assert store.count("boom_on_odd") == 2  # only the evens landed

        # The odd inputs must run again; the even ones must not be touched.
        def resumed(x):
            if x % 2 == 0:
                raise AssertionError(f"{x} should have been restored")
            return x * 10

        with ThreadQueue(workers=4) as q:
            grp = q.group(resumed, [1, 2, 3, 4], checkpoint=store, namespace="boom_on_odd")
            q.run(strict=True)
        assert grp.values() == (10, 20, 30, 40)

    def test_ordering_is_preserved(self, store):
        store.record("double", canonical_key(2), 999)
        with ThreadQueue(workers=4) as q:
            grp = q.group(double, [1, 2, 3], checkpoint=store)
            q.run(strict=True)
        assert grp.values() == (2, 999, 6)

    def test_failures_are_not_recorded(self, store):
        with ThreadQueue(workers=2) as q:
            q.map(boom_on_odd, [1, 3], checkpoint=store)
            q.run()
        assert store.count("boom_on_odd") == 0

    def test_cancelled_tasks_are_not_recorded(self, store):
        with ThreadQueue(workers=1) as q:
            handles = q.map(double, [1, 2], checkpoint=store)
            handles[0].cancel()
            handles[1].cancel()
            q.run()
        assert store.count("double") == 0

    def test_restored_result_is_marked(self, store):
        store.record("double", canonical_key(1), 42)
        with ThreadQueue(workers=1) as q:
            handles = q.map(double, [1], checkpoint=store)
            q.run()
        result = handles[0].result()
        assert result.status == "succeeded"
        assert result.value == 42
        assert result.attempts == 0  # never executed this run
        assert result.message == "restored from checkpoint"

    def test_restored_tasks_stay_out_of_the_run_summary(self, store):
        store.record("double", canonical_key(1), 42)
        with ThreadQueue(workers=2) as q:
            q.map(double, [1, 2], checkpoint=store)
            summary = q.run(strict=True)
        # Only the work actually performed this run is summarised.
        assert summary.total_submitted == 1
        assert summary.values == (4,)

    def test_fully_restored_run_submits_nothing(self, store):
        for x in (1, 2, 3):
            store.record("never_call", canonical_key(x), x)
        with ThreadQueue(workers=2) as q:
            grp = q.group(never_call, [1, 2, 3], checkpoint=store)
            summary = q.run(strict=True)
        assert summary.total_submitted == 0
        assert grp.values() == (1, 2, 3)

    def test_custom_key(self, store):
        rows = [{"id": 1, "seen": "a"}, {"id": 2, "seen": "b"}]
        store.record("by_id", "1", "cached")
        with ThreadQueue(workers=2) as q:
            grp = q.group(lambda **kw: kw["seen"], rows, checkpoint=store, key=lambda r: r["id"], namespace="by_id")
            q.run(strict=True)
        assert grp.values() == ("cached", "b")

    def test_key_required_for_non_json_inputs(self, store):
        with ThreadQueue(workers=1) as q, pytest.raises(TypeError, match="pass key="):
            q.map(double, [object()], checkpoint=store)

    def test_namespace_defaults_to_callable_name(self, store):
        with ThreadQueue(workers=1) as q:
            q.map(double, [1], checkpoint=store)
            q.run()
        assert store.namespaces() == ("double",)

    def test_different_callables_do_not_collide(self, store):
        with ThreadQueue(workers=2) as q:
            q.map(double, [2], checkpoint=store)
            q.run()
        # Same input, different callable — must still run.
        with ThreadQueue(workers=2) as q:
            grp = q.group(boom_on_odd, [2], checkpoint=store)
            q.run(strict=True)
        assert grp.values() == (20,)

    def test_store_values_false_restores_none(self, tmp_path):
        with Checkpoint(tmp_path / "c.sqlite", store_values=False) as cp:
            with ThreadQueue(workers=2) as q:
                q.map(double, [1, 2], checkpoint=cp)
                q.run(strict=True)
            with ThreadQueue(workers=2) as q:
                handles = q.map(never_call, [1, 2], checkpoint=cp, namespace="double")
                q.run(strict=True)
            assert [h.value() for h in handles] == [None, None]
            assert handles[0].result().message.endswith("(value not retained)")


class TestGroupForms:
    def test_heterogeneous_group_requires_namespace(self, store):
        with ThreadQueue(workers=1) as q, pytest.raises(ValueError, match="needs a stable name"):
            q.group([(add, 1, 2)], checkpoint=store)

    def test_heterogeneous_group_with_namespace(self, store):
        by_args = {"checkpoint": store, "namespace": "etl", "key": lambda e: e[1:]}
        with ThreadQueue(workers=2) as q:
            q.group([(add, 1, 2), (double, 5)], **by_args)
            q.run(strict=True)
        assert store.count("etl") == 2

        # Same keys, different callables: keying on args alone means these restore.
        with ThreadQueue(workers=2) as q:
            grp = q.group([(never_call, 1, 2), (never_call, 5)], **by_args)
            q.run(strict=True)
        assert grp.values() == (3, 10)

    def test_default_heterogeneous_key_includes_the_callable(self, store):
        with ThreadQueue(workers=1) as q:
            q.group([(double, 5)], checkpoint=store, namespace="etl")
            q.run(strict=True)
        # Same args, different callable — the default key must not treat these as the same work.
        with ThreadQueue(workers=1) as q:
            grp = q.group([(boom_on_odd, 5)], checkpoint=store, namespace="etl")
            q.run()
        assert grp.wait().failed == 1

    def test_heterogeneous_group_accepts_explicit_group_id(self, store):
        with ThreadQueue(workers=1) as q:
            q.group([(add, 1, 2)], checkpoint=store, group_id="etl-1")
            q.run(strict=True)
        assert store.namespaces() == ("etl-1",)

    def test_heterogeneous_group_keys_include_callable_name(self, store):
        with ThreadQueue(workers=2) as q:
            grp = q.group([(add, 1, 2), (add, 1, 2)], checkpoint=store, namespace="etl")
            q.run(strict=True)
        assert grp.values() == (3, 3)
        assert store.count("etl") == 1  # identical entries share one key

    def test_bad_entry_still_rejected(self, store):
        with ThreadQueue(workers=1) as q, pytest.raises(TypeError, match="must be a tuple"):
            q.group(["not-a-tuple"], checkpoint=store, namespace="etl")


class TestBoundTask:
    def test_bound_map_accepts_checkpoint(self, store):
        with ThreadQueue(workers=2) as q:
            worker = q.task(name="bound")(double)
            worker.map([1, 2], checkpoint=store, namespace="bound")
            q.run(strict=True)
        assert store.count("bound") == 2

    def test_bound_group_resumes(self, store):
        store.record("bound", canonical_key(1), 111)
        with ThreadQueue(workers=2) as q:
            worker = q.task()(double)
            grp = worker.group([1, 2], checkpoint=store, namespace="bound")
            q.run(strict=True)
        assert grp.values() == (111, 4)


class TestAsyncQueue:
    async def test_resume(self, store):
        async with AsyncQueue(workers=4) as q:
            q.map(boom_on_odd, [1, 2, 3, 4], checkpoint=store)
            await q.run()
        assert store.count("boom_on_odd") == 2

        async with AsyncQueue(workers=4) as q:
            grp = q.group(never_call, [2, 4], checkpoint=store, namespace="boom_on_odd")
            await q.run(strict=True)
        assert await grp.values() == (20, 40)

    async def test_amap_with_checkpoint(self, store):
        assert await amap(double, [1, 2, 3], checkpoint=store, workers=2) == (2, 4, 6)
        assert await amap(never_call, [1, 2, 3], checkpoint=store, namespace="double", workers=2) == (2, 4, 6)

    async def test_amap_partial_failure_then_resume(self, store):
        with pytest.raises(ExecutionError):
            await amap(boom_on_odd, [1, 2], checkpoint=store, workers=2)
        assert store.lookup("boom_on_odd", [canonical_key(2)])[canonical_key(2)].value == 20


class TestSyncShortcuts:
    def test_tmap_with_checkpoint(self, store):
        assert tmap(double, [1, 2, 3], checkpoint=store, workers=2) == (2, 4, 6)
        assert tmap(never_call, [1, 2, 3], checkpoint=store, namespace="double", workers=2) == (2, 4, 6)

    def test_pmap_with_checkpoint(self, store):
        assert pmap(double, [1, 2, 3], checkpoint=store, workers=2) == (2, 4, 6)
        assert pmap(never_call, [1, 2, 3], checkpoint=store, namespace="double", workers=2) == (2, 4, 6)


class TestProcessQueue:
    def test_resume_skips_recorded_inputs(self, store):
        with ProcessQueue(workers=2) as q:
            q.map(boom_on_odd, [1, 2, 3, 4], checkpoint=store)
            q.run()
        assert store.count("boom_on_odd") == 2

        # never_call would raise in the subprocess if these were actually submitted.
        with ProcessQueue(workers=2) as q:
            grp = q.group(never_call, [2, 4], checkpoint=store, namespace="boom_on_odd")
            q.run(strict=True)
        assert grp.values() == (20, 40)
