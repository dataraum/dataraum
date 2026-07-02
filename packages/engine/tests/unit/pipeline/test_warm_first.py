"""Tests for the warm-first fan-out stagger (DAT-601).

The load-bearing property: the FIRST item's call completes before any other
item is submitted, so its finished request has committed the shared
prompt-cache prefix that the rest then read.
"""

from __future__ import annotations

import threading
import time
from concurrent.futures import Future, ThreadPoolExecutor, as_completed

from dataraum.pipeline.phases._warm_first import submit_warm_first


def test_first_completes_before_rest_start() -> None:
    first_finished_at: list[float] = []
    later_started_at: list[float] = []
    lock = threading.Lock()

    def work(key: int) -> int:
        if key == 0:
            time.sleep(0.05)
            with lock:
                first_finished_at.append(time.monotonic())
        else:
            with lock:
                later_started_at.append(time.monotonic())
        return key

    with ThreadPoolExecutor(max_workers=4) as pool:

        def submit(key: int) -> Future[int]:
            return pool.submit(work, key)

        futures = submit_warm_first(submit, list(range(6)))
        results = sorted(futures[f] for f in as_completed(futures) if f.result() is not None)

    assert results == [0, 1, 2, 3, 4, 5]
    assert len(first_finished_at) == 1 and len(later_started_at) == 5
    assert min(later_started_at) >= first_finished_at[0]


def test_single_item_and_empty() -> None:
    with ThreadPoolExecutor(max_workers=2) as pool:

        def submit(key: str) -> Future[str]:
            return pool.submit(lambda: key)

        assert submit_warm_first(submit, []) == {}
        futures = submit_warm_first(submit, ["only"])
        assert [f.result() for f in futures] == ["only"]


def test_first_failure_surfaces_in_collect_not_at_submit() -> None:
    # wait() must not consume the first future's exception — the caller's
    # collect loop owns per-future failure handling, exactly as unstaggered.
    def work(key: int) -> int:
        if key == 0:
            raise ValueError("boom")
        return key

    with ThreadPoolExecutor(max_workers=2) as pool:

        def submit(key: int) -> Future[int]:
            return pool.submit(work, key)

        futures = submit_warm_first(submit, [0, 1])  # must NOT raise here
        outcomes: dict[int, str] = {}
        for f in as_completed(futures):
            try:
                outcomes[futures[f]] = str(f.result())
            except ValueError as exc:
                outcomes[futures[f]] = f"error: {exc}"

    assert outcomes == {0: "error: boom", 1: "1"}
