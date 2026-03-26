"""SSTable (Sorted String Table) package.

Provides on-disk sorted key-value storage with:

- :class:`~app.sstable.writer.SSTableWriter` — write-once builder
- :class:`~app.sstable.reader.SSTableReader` — mmap-backed reader with lazy bloom/index loading
- :class:`~app.sstable.meta.SSTableMeta` — immutable metadata for each table
- :class:`~app.sstable.registry.SSTableRegistry` — ref-counted reader lifecycle management
"""
