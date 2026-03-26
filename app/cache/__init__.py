"""Block cache package.

Provides :class:`~app.cache.block.BlockCache`, a three-tier LRU cache
that stores SSTable data blocks, sparse indexes, and bloom filters with
independent eviction policies per tier.
"""
