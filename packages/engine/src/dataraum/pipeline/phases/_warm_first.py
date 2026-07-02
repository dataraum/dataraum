"""Warm-first submission for concurrent identical-prefix LLM fan-outs (DAT-601).

Concurrent LLM calls that share a byte-identical prompt prefix (same tools +
system per phase template) cannot read a prompt-cache entry that is still being
written — an unstaggered burst pays the cache write N times over. Measured on
the finance smoke workspace: metrics warming fired 11 calls in one generation
at cap 10 and wrote the same ~6.9k-token prefix 10 times (only call 11 read);
validation's first wave of 4 did the same at its cap.

Letting the FIRST call run to completion commits the shared prefix; every call
released after it reads the entry instead. Wall-clock cost is ~zero whenever
the item count exceeds the pool cap (the wave count is unchanged) and one
call-time otherwise.

This is deliberately the ONLY place the stagger lives: metrics warming and
validation are the engine's only two concurrent LLM fan-outs (every other
ThreadPoolExecutor is pure statistics; all other LLM phases are single batched
calls).
"""

from __future__ import annotations

from collections.abc import Callable, Sequence
from concurrent.futures import Future, wait


def submit_warm_first[K, R](
    submit: Callable[[K], Future[R]],
    keys: Sequence[K],
) -> dict[Future[R], K]:
    """Submit ``keys[0]``, wait for it to finish, then submit the rest.

    The first future is awaited with :func:`concurrent.futures.wait`, which
    does NOT consume its exception — the caller's ``as_completed`` collect
    loop sees exactly the same per-future success/failure semantics as an
    unstaggered submit.

    Args:
        submit: Submits one key's work to the caller's pool, returning its
            future (a closure over ``pool.submit(fn, …)``).
        keys: Work items in submission order; ``keys[0]`` is the cache warmer.

    Returns:
        ``{future: key}`` for the caller's collect loop (the first key's
        completed future included).
    """
    ordered = list(keys)
    futures: dict[Future[R], K] = {}
    if not ordered:
        return futures
    first = submit(ordered[0])
    futures[first] = ordered[0]
    if len(ordered) > 1:
        wait([first])
        for key in ordered[1:]:
            futures[submit(key)] = key
    return futures
