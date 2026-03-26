"""Compaction package.

Provides the data structures and worker logic for merging SSTables
across levels. :class:`~app.compaction.task.CompactionTask` describes
a single job, and :func:`~app.compaction.worker.run_compaction`
executes the merge in a subprocess.
"""
