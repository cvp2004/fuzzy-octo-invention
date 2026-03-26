"""Bloom filter package.

Provides :class:`~app.bloom.filter.BloomFilter`, an mmh3-backed
probabilistic membership test used by SSTable readers to skip disk
reads for keys that are definitely not present.
"""
