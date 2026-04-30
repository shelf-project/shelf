"""Cache eviction / admission policies for the SHELF-35 replay harness.

Every policy implements the [`Policy`][.] protocol — an opinionated,
intentionally-narrow surface that:

* admits (or rejects) every access into the cache,
* records a hit (so the policy can update its bookkeeping),
* picks a victim when the cache is full,
* names itself for the per-policy TSV output filename.

Policies operate on the same [`CacheState`][.] handle the simulator
maintains, which exposes the keys → sizes map plus a few primitives
(``promote``, ``insert``, ``evict``) so policies don't reach into the
simulator's private state.

Policies in v1
--------------

* [`LRU`][.] — baseline.
* [`FIFO`][.] — sanity floor; if Sieve / TinyLFU don't beat FIFO, the
  trace is degenerate (all-hot or all-cold).
* [`S3FIFO`][.] — Foyer's pre-bump default; included for parity with
  rep-1's pre-rc.5 production policy. The small-queue bypass class
  that drove the preview-3 LRU revert is modelled here so the harness
  can demonstrate the gap Sieve closes upstream of a real Foyer bump.
* [`BeladyMin`][.] — future-optimal oracle. Establishes the upper
  bound every other policy is measured against. Pre-computes a
  forward-index over the trace so victim selection is O(log n) per
  eviction.

Sieve / W-TinyLFU / 3L-Cache are deliberately deferred to SHELF-35b —
they each warrant their own ADR and their own per-policy TSV; v1 of
the harness is the *infrastructure*, not every algorithm.
"""
from __future__ import annotations

from collections import OrderedDict, deque
from dataclasses import dataclass, field
from typing import Callable, Deque, Dict, List, Optional, Protocol, Tuple

from .trace import Access


class Policy(Protocol):
    """Cache-replacement policy interface.

    Implementations MUST be deterministic given a fixed access trace
    and capacity — the harness's correctness rests on exact reproducibility
    so two operators running the same trace get the same TSV.
    """

    name: str
    """Short identifier used as the per-policy TSV filename suffix."""

    def reset(self) -> None:
        """Clear all internal state (used at the start of each
        capacity sweep)."""

    def on_hit(self, access: Access) -> None:
        """Called when ``access.object_id`` was already cached."""

    def on_admit(self, access: Access) -> None:
        """Called when the simulator inserts ``access.object_id`` after
        a miss + capacity has space. The policy may choose to record
        this for future victim selection."""

    def on_evict(self, victim: str) -> None:
        """Called when the simulator has just evicted ``victim`` to
        make room for an incoming object."""

    def select_victim(self, cached: Dict[str, int]) -> Optional[str]:
        """Return the key the simulator should evict next. ``cached``
        maps key → size_bytes. Returning ``None`` is a degenerate
        signal that the policy has no candidates — the simulator
        treats this as a forced-bypass (object too large to admit at
        all)."""

    def admit(self, access: Access, cached: Dict[str, int], capacity_bytes: int) -> bool:  # noqa: D401, E501
        """Should the policy admit ``access`` into the cache?

        Default v1 behaviour is "admit if it fits at all". W-TinyLFU /
        3L-Cache will override this in SHELF-35b.
        """
        return access.size_bytes <= capacity_bytes


# ---------- LRU --------------------------------------------------------------


class LRU:
    """Plain Least-Recently-Used. ``OrderedDict`` move_to_end on hit."""

    name = "lru"

    def __init__(self) -> None:
        self._order: "OrderedDict[str, None]" = OrderedDict()

    def reset(self) -> None:
        self._order.clear()

    def on_hit(self, access: Access) -> None:
        self._order.move_to_end(access.object_id, last=True)

    def on_admit(self, access: Access) -> None:
        self._order[access.object_id] = None
        self._order.move_to_end(access.object_id, last=True)

    def on_evict(self, victim: str) -> None:
        self._order.pop(victim, None)

    def select_victim(self, cached: Dict[str, int]) -> Optional[str]:
        # Drop stale entries the simulator already evicted.
        while self._order and next(iter(self._order)) not in cached:
            self._order.popitem(last=False)
        if not self._order:
            return next(iter(cached), None)
        return next(iter(self._order))

    def admit(self, access: Access, cached: Dict[str, int], capacity_bytes: int) -> bool:
        return access.size_bytes <= capacity_bytes


# ---------- FIFO -------------------------------------------------------------


class FIFO:
    """First-In, First-Out. No promotion on hit."""

    name = "fifo"

    def __init__(self) -> None:
        self._q: Deque[str] = deque()
        self._present: set[str] = set()

    def reset(self) -> None:
        self._q.clear()
        self._present.clear()

    def on_hit(self, access: Access) -> None:  # FIFO ignores hits
        return

    def on_admit(self, access: Access) -> None:
        if access.object_id not in self._present:
            self._q.append(access.object_id)
            self._present.add(access.object_id)

    def on_evict(self, victim: str) -> None:
        self._present.discard(victim)
        # Lazy-remove from queue head; select_victim already pops.

    def select_victim(self, cached: Dict[str, int]) -> Optional[str]:
        # Skip queue entries the simulator already evicted.
        while self._q:
            head = self._q[0]
            if head in cached:
                return head
            self._q.popleft()
            self._present.discard(head)
        return next(iter(cached), None)

    def admit(self, access: Access, cached: Dict[str, int], capacity_bytes: int) -> bool:
        return access.size_bytes <= capacity_bytes


# ---------- S3-FIFO (production-shape; small-queue gate modelled) -----------


class S3FIFO:
    """S3-FIFO with a small-queue + main-queue split, modelled to
    reproduce the preview-3 production trade-off.

    See ``shelfd/src/store.rs:463`` for the live default config the
    rep-1 v0.1 binary used (``small_queue_capacity_ratio = 0.1``,
    ``small_to_main_freq_threshold = 1``). Reproducing the same gate
    here makes the harness's "Sieve beats S3-FIFO by N pp" claim
    measurable against the same workload, without needing to flash
    a Foyer 0.18+ binary on cluster.

    Reference: Yang et al., **"FIFO queues are all you need for cache
    eviction"**, SOSP 2023, https://www.usenix.org/system/files/nsdi24-zhang-yazhuo.pdf .
    """

    name = "s3fifo"

    def __init__(
        self,
        small_queue_ratio: float = 0.1,
        promotion_threshold: int = 1,
    ) -> None:
        self._small_q: Deque[str] = deque()
        self._main_q: Deque[str] = deque()
        self._freq: Dict[str, int] = {}
        self._small_ratio = small_queue_ratio
        self._promotion = promotion_threshold

    def reset(self) -> None:
        self._small_q.clear()
        self._main_q.clear()
        self._freq.clear()

    def on_hit(self, access: Access) -> None:
        self._freq[access.object_id] = self._freq.get(access.object_id, 0) + 1

    def on_admit(self, access: Access) -> None:
        # New entries land on the small queue; promotion happens
        # lazily when select_victim sweeps and finds a small-queue
        # head whose freq exceeds the promotion threshold.
        self._small_q.append(access.object_id)
        self._freq.setdefault(access.object_id, 0)

    def on_evict(self, victim: str) -> None:
        self._freq.pop(victim, None)

    def select_victim(self, cached: Dict[str, int]) -> Optional[str]:
        # Phase 1: drain small-queue heads until we find one whose
        # freq < promotion_threshold (eviction candidate) — anything
        # above that bar gets promoted to main_q.
        while self._small_q:
            head = self._small_q[0]
            if head not in cached:
                self._small_q.popleft()
                continue
            if self._freq.get(head, 0) >= self._promotion:
                self._small_q.popleft()
                self._main_q.append(head)
                continue
            return head
        # Phase 2: small queue empty — evict from main_q head.
        while self._main_q:
            head = self._main_q[0]
            if head in cached:
                return head
            self._main_q.popleft()
        return next(iter(cached), None)

    def admit(self, access: Access, cached: Dict[str, int], capacity_bytes: int) -> bool:
        return access.size_bytes <= capacity_bytes


# ---------- Belady-MIN -------------------------------------------------------


@dataclass
class BeladyMin:
    """Future-optimal oracle. Evicts the cached object whose **next**
    access is furthest in the future (or which has no future access).

    The simulator pre-supplies the trace before the run starts; we
    materialise a per-key list of future timestamps so victim
    selection is one heap probe per cached key rather than a forward
    scan of the trace. For traces of N accesses across K distinct
    keys this gives O(N log K) per simulation rather than O(N²).

    This is the **upper bound** every other policy is measured
    against. The headline number in the SHELF-35 TSV is "Sieve is X
    percentage points below Belady-MIN" — the smaller X, the less
    headroom remains for a learned policy (SHELF-36 / 3L-Cache).
    """

    name: str = "belady"

    # Forward-index: object_id → list of access indices, monotonically
    # increasing. We track a per-key "next pointer" that advances on
    # every hit so future-distance is O(1) per key during eviction.
    _index: Dict[str, List[int]] = field(default_factory=dict)
    _ptr: Dict[str, int] = field(default_factory=dict)
    _now: int = 0

    def prepare(self, accesses: List[Access]) -> None:
        """Pre-compute the forward index. Called once before
        simulation; not part of the [`Policy`][.] protocol because
        only Belady needs it."""
        self._index = {}
        for i, a in enumerate(accesses):
            self._index.setdefault(a.object_id, []).append(i)
        self._ptr = {k: 0 for k in self._index}
        self._now = 0

    def reset(self) -> None:
        # Reset advances all pointers back to 0; the caller is
        # responsible for re-prepare()-ing if the trace changed.
        for k in self._ptr:
            self._ptr[k] = 0
        self._now = 0

    def on_hit(self, access: Access) -> None:
        # Advance this key's pointer past `self._now`.
        self._advance(access.object_id)

    def on_admit(self, access: Access) -> None:
        self._advance(access.object_id)

    def on_evict(self, victim: str) -> None:
        # Eviction does not advance the pointer — Belady evaluates the
        # *next* future access from the current trace position.
        return

    def select_victim(self, cached: Dict[str, int]) -> Optional[str]:
        # Pick the key whose next future access is furthest (or absent).
        worst_key: Optional[str] = None
        worst_next: int = -1  # -1 sentinel = no future access (best to evict)
        for key in cached:
            ptr = self._ptr.get(key, 0)
            future = self._index.get(key, [])
            # Skip past indices ≤ self._now.
            while ptr < len(future) and future[ptr] <= self._now:
                ptr += 1
            self._ptr[key] = ptr
            if ptr >= len(future):
                # No future access. Best possible victim.
                return key
            next_idx = future[ptr]
            if next_idx > worst_next:
                worst_next = next_idx
                worst_key = key
        return worst_key

    def admit(self, access: Access, cached: Dict[str, int], capacity_bytes: int) -> bool:
        return access.size_bytes <= capacity_bytes

    def step(self, idx: int) -> None:
        """Called by the simulator after each access so Belady knows
        the current trace position. Not part of the [`Policy`][.]
        protocol — Belady-specific."""
        self._now = idx

    def _advance(self, key: str) -> None:
        ptr = self._ptr.get(key, 0)
        future = self._index.get(key, [])
        while ptr < len(future) and future[ptr] <= self._now:
            ptr += 1
        self._ptr[key] = ptr


def build_policy(
    name: str,
    accesses: List[Access],
) -> Policy:
    """Factory; matches the CLI's ``--policies`` flag.

    Belady receives the trace at construction time (forward-index
    requires the entire trace). Other policies are construction-free.
    """
    name = name.strip().lower()
    if name == "lru":
        return LRU()
    if name == "fifo":
        return FIFO()
    if name == "s3fifo":
        return S3FIFO()
    if name == "belady":
        b = BeladyMin()
        b.prepare(accesses)
        return b
    raise ValueError(f"unknown policy: {name!r}")
