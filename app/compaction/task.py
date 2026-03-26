"""CompactionTask — serialisable description of one compaction job.

All fields are primitive types (str, int, list, dict) so the task
can be passed across the ProcessPoolExecutor subprocess boundary.
No live objects, no file handles, no locks.
"""

from __future__ import annotations

from dataclasses import dataclass

from app.types import FileID, Level, SeqNum


@dataclass(frozen=True)
class CompactionTask:
    """Immutable description of one compaction job.

    All fields are primitive types so the task can be safely passed
    across the ``ProcessPoolExecutor`` subprocess boundary.

    Attributes:
        task_id: Unique identifier for this compaction run.
        input_file_ids: SSTable file IDs to merge (includes source and
            destination level files).
        input_dirs: Mapping from file ID to its on-disk directory path.
        output_file_id: File ID for the newly created merged SSTable.
        output_dir: Directory where the output SSTable will be written.
        output_level: Target compaction level for the output.
        seq_cutoff: Tombstones with ``seq < seq_cutoff`` are garbage-
            collected during the merge.
        bloom_fpr: Target false positive rate for the output bloom filter.
    """

    task_id: str
    input_file_ids: list[FileID]
    input_dirs: dict[FileID, str]
    output_file_id: FileID
    output_dir: str
    output_level: Level
    seq_cutoff: SeqNum
    bloom_fpr: float = 0.01
