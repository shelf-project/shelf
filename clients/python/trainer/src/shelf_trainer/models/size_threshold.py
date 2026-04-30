"""Size-threshold admission policy (v1 default, per ADR-0003).

This is the only real admission model in v1: refuse objects
``>= size_threshold_bytes`` unless the key is on the pin list. Everything
else is admitted. The Rust ``shelfd`` implements the production copy; this
Python class exists so the trainer's offline replay harness can score a
candidate LightGBM model against the v1 default on an identical codepath.

Intentionally:

- No learning / no state beyond constructor args.
- No I/O. Call sites load the pin list (a set of table identifiers) from S3.
- Pure function ``admit(size, pinned)``; unit-testable on a laptop.
"""

from __future__ import annotations

from dataclasses import dataclass

_DEFAULT_THRESHOLD_BYTES = 1 << 30  # 1 GiB, matches shelfd default (SHELF-25).


@dataclass(frozen=True, slots=True)
class SizeThresholdAdmission:
    """Refuse objects at-or-above ``size_threshold_bytes`` unless pinned.

    Parameters
    ----------
    size_threshold_bytes:
        Inclusive refuse boundary. Objects strictly smaller than this are
        always admitted. Default is 1 GiB (``2**30``), matching shelfd's
        ``shelf.admission.size_threshold_mib=1024`` default from SHELF-25.
    pinned_bypass:
        If ``True`` (default), a pinned object is admitted regardless of
        size. Set to ``False`` in hostile tests that want the pure
        size rule.
    """

    size_threshold_bytes: int = _DEFAULT_THRESHOLD_BYTES
    pinned_bypass: bool = True

    def __post_init__(self) -> None:
        if self.size_threshold_bytes <= 0:
            raise ValueError(
                f"size_threshold_bytes must be positive, got {self.size_threshold_bytes}"
            )

    def admit(self, size: int, pinned: bool) -> bool:
        """Return ``True`` iff the object should be inserted into Shelf's NVMe tier.

        Parameters
        ----------
        size:
            Object (or row-group byte-range) size in bytes. Must be non-negative.
        pinned:
            ``True`` iff the key's table/partition is on the pin list.

        Returns
        -------
        bool
            ``True`` to admit, ``False`` to refuse (read-through but don't
            populate the cache).

        Raises
        ------
        ValueError
            If ``size`` is negative.
        """
        if size < 0:
            raise ValueError(f"size must be non-negative, got {size}")
        if pinned and self.pinned_bypass:
            return True
        return size < self.size_threshold_bytes
