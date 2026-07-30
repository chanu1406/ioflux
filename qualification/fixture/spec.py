"""Pinned constants for qualification fixture qual-01.

Every number here is part of the fixture contract. Changing any of them
produces a different fixture and invalidates comparison against archived
qual-01 evidence.

SHARD_BYTES is deliberately NOT a multiple of BLOCK_BYTES: the final read of
every shard requests BLOCK_BYTES and returns fewer, which is what exercises the
requested-vs-returned dimension of §10.1. An aligned size would hide it.
"""

FIXTURE_ID = "qual-01"

SHARDS = 32
SHARD_BYTES = 8_500_000
BLOCK_BYTES = 262_144
WORKERS = 4

# Per-block "decode" stand-in: sum every COMPUTE_STRIDE-th byte of the block.
# Bounded, deterministic in work, and enough to make the reader closed-loop
# rather than a pure syscall generator.
COMPUTE_STRIDE = 64

DATASET_TOTAL_BYTES = SHARDS * SHARD_BYTES


def shard_name(i: int) -> str:
    return f"shard_{i:04d}.bin"


def full_blocks_per_shard() -> int:
    return SHARD_BYTES // BLOCK_BYTES


def tail_bytes_per_shard() -> int:
    return SHARD_BYTES % BLOCK_BYTES


def shards_for_worker(worker: int):
    """Static, deterministic shard assignment: worker w owns shards[w::WORKERS]."""
    return list(range(SHARDS))[worker::WORKERS]
