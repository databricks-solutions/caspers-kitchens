"""Small print helpers for stage / job status output.

Why this module exists:

Stages and helpers across the repo print operator-visible status lines like
``"✅ Created warehouse: ..."`` or ``"♻️ Reusing existing project"``.  Emojis
render fine in the Databricks notebook UI and a modern terminal, but show up
as garbled boxes in some CI log viewers and screen readers.

Using ``status.ok(...)`` / ``status.reuse(...)`` instead of bare
``print("✅ ...")`` localizes the rendering decision:

- In the default state, output is identical to today's emoji prints.
- Set ``CASPERS_NO_EMOJI=1`` (or ``true`` / ``yes`` / ``on``) in the job
  environment to swap emojis for ASCII tags (``[OK]``, ``[REUSE]``, ``[WARN]``,
  ``[FAIL]``, ``[RUN]``, ``[INFO]``, ``[SKIP]``, ``[SEARCH]``, ``[DROP]``).

The helpers are intentionally tiny and free of any Databricks-specific
dependency so they can be imported from notebooks, app code, and unit tests.
"""

from __future__ import annotations

import os
import sys
from typing import IO, Optional

_ASCII_ENV_VAR = "CASPERS_NO_EMOJI"

# Single source of truth for the (emoji, ascii-tag) pair per status level.
# Add a new level here and a one-line wrapper below if you need it.
_LEVELS: dict[str, tuple[str, str]] = {
    "ok":     ("✅",  "[OK]"),
    "reuse":  ("♻️",  "[REUSE]"),
    "warn":   ("⚠️",  "[WARN]"),
    "fail":   ("❌",  "[FAIL]"),
    "run":    ("🚀",  "[RUN]"),
    "info":   ("ℹ️",  "[INFO]"),
    "skip":   ("⏭️",  "[SKIP]"),
    "search": ("🔎",  "[SEARCH]"),
    "drop":   ("🗑️",  "[DROP]"),
}


def _ascii_mode() -> bool:
    return os.environ.get(_ASCII_ENV_VAR, "").lower() in ("1", "true", "yes", "on")


def _prefix(level: str) -> str:
    emoji, ascii_tag = _LEVELS[level]
    return ascii_tag if _ascii_mode() else emoji


def _emit(level: str, msg: str, *, file: Optional[IO[str]] = None) -> None:
    print(f"{_prefix(level)} {msg}", file=file or sys.stdout, flush=True)


def ok(msg: str) -> None:
    """Success / created.  Replaces ``print("✅ ...")``."""
    _emit("ok", msg)


def reuse(msg: str) -> None:
    """Idempotent reuse of an existing resource.  Replaces ``print("♻️ ...")``."""
    _emit("reuse", msg)


def warn(msg: str) -> None:
    """Non-fatal warning.  Replaces ``print("⚠️ ...")``.  Goes to stderr."""
    _emit("warn", msg, file=sys.stderr)


def fail(msg: str) -> None:
    """Fatal-ish error message.  Replaces ``print("❌ ...")``.  Goes to stderr."""
    _emit("fail", msg, file=sys.stderr)


def run(msg: str) -> None:
    """Triggered an async job / pipeline run.  Replaces ``print("🚀 ...")``."""
    _emit("run", msg)


def info(msg: str) -> None:
    """Neutral progress / configuration line.  Replaces ``print("ℹ️ ...")``."""
    _emit("info", msg)


def skip(msg: str) -> None:
    """Step intentionally skipped.  Replaces ``print("⏭️ ...")``."""
    _emit("skip", msg)


def search(msg: str) -> None:
    """Scanning / sweep step.  Replaces ``print("🔎 ...")``."""
    _emit("search", msg)


def drop(msg: str) -> None:
    """Dropped a stale resource.  Replaces ``print("🗑️ ...")``."""
    _emit("drop", msg)


__all__ = [
    "ok", "reuse", "warn", "fail", "run", "info", "skip", "search", "drop",
]
