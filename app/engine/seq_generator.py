"""SeqGenerator — thread-safe monotonic sequence number generator.

A standalone component owned by :class:`LSMEngine`.  The generated
sequence number is passed to both WALManager and MemTableManager —
neither owns the counter.
"""

from __future__ import annotations

import threading

from app.observability import get_logger
from app.types import SeqNum

logger = get_logger(__name__)


class SeqGenerator:
    """Thread-safe, monotonically increasing sequence number generator.

    Each call to :meth:`next` returns a strictly higher value than
    the previous call.  Use :meth:`restore` at startup to set the
    counter from the highest seq seen in the WAL or SSTables.
    """

    def __init__(self, start: int = 0) -> None:
        self._seq = start
        self._lock = threading.Lock()
        logger.info("SeqGenerator created", start=start)

    def next(self) -> SeqNum:
        """Atomically increment and return the next sequence number."""
        with self._lock:
            self._seq += 1
            return self._seq

    def restore(self, max_seen: int) -> None:
        """Set the counter to at least *max_seen*.

        Called during recovery — ensures the next generated seq is
        strictly greater than anything already on disk or in the WAL.
        """
        if max_seen < 0:
            logger.warning(
                "SeqGenerator restore ignored negative value",
                max_seen=max_seen,
            )
            return

        with self._lock:
            old = self._seq
            self._seq = max(self._seq, max_seen)
            logger.info(
                "SeqGenerator restored",
                old=old,
                new=self._seq,
                gap=self._seq - old,
            )

    @property
    def current(self) -> SeqNum:
        """Return the current seq value (last generated, or 0 if none)."""
        with self._lock:
            return self._seq
