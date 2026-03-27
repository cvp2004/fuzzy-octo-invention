"""SSTable (Sorted String Table) package.

Provides on-disk sorted key-value storage with:

- :class:`~app.sstable.writer.SSTableWriter` — write-once builder
- :class:`~app.sstable.reader.SSTableReader` — mmap-backed reader
- :class:`~app.sstable.meta.SSTableMeta` — immutable metadata
- :class:`~app.sstable.registry.SSTableRegistry` — ref-counted readers
"""
