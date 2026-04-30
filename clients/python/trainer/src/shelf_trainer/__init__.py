"""Shelf trainer package.

Nightly jobs that emit:

- ``pin_list.json`` (v1; top-N tables by access frequency intersection).
- ``admission_v<N>.txt`` (v1.x LightGBM model; *stub only* in this skeleton —
  see ADR-0003 and ``docs/labels.md``).
- ``bloom_recommendations.json`` (Phase 8, not scaffolded here).
- ``mv_candidates.json`` (Phase 9, not scaffolded here).

The only real logic in v1 is :mod:`shelf_trainer.models.size_threshold` —
everything else is a stub that raises ``NotImplementedError`` with a ticket
reference (``SHELF-NN``) so agent-8 / ops can trace which ticket owns the
implementation.
"""

from __future__ import annotations

__version__ = "0.1.0"

__all__ = ["__version__"]
