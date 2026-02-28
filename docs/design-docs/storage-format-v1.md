# Repository Storage Format v1 (CBT + Chunk + Pack + Index)

## Purpose

Define a concrete on-disk repository format for RBD backup data when using
Kubernetes SnapshotMetadataService (CBT metadata: offset/length).

This document fixes the v1 storage layout and record format for:
- generation management
- deduplicated chunk storage
- restore path
- retention and GC

## Inputs and Constraints

- source is block device data
- changed range metadata comes from CBT (`offset`, `length`)
- destination is plain filesystem files
- data transfer must be incremental
- retention is generation-based (for example keep 7 days)

## Design Decision

v1 uses `chunk + pack + index + snapshot(manifest)`.

Why:
- avoids file explosion of chunk-per-file
- supports dedup across generations
- keeps restore random access practical via index
- supports retention and GC with clear live-set semantics

## Why Not Use `rbd export-diff` As Repository Format

`export-diff` format is good as a transport stream format, but is not a good
internal repository format for long-lived deduplicated generations.

- it describes a diff stream, not a global chunk address space
- it does not provide stable content-addressed chunk identity
- retention pruning would depend on deep diff chains
- pack compaction and chunk-level GC are harder

What we reuse from that style:
- append-oriented records
- clear metadata/data separation
- versioned binary encoding with explicit lengths

## Fixed v1 Parameters

- chunk size: `4 MiB` (logical restore/write unit)
- chunking mode: fixed-size chunking
- chunk ID: `SHA-256(raw chunk bytes)` (32 bytes)
- pack target size: `512 MiB`
- compression: optional `zstd` per chunk payload
- encryption: not in v1 format core (handled outside format)

The chunking spec is immutable per repository.

## Repository Tree

```text
repo/
  repo.json
  refs/
    latest.txt
  snapshots/
    2026-02-23T01-00-00Z/
      manifest.json
      seg-alloc.bin.zst
      seg-change.bin.zst
    2026-02-24T01-00-00Z/
      manifest.json
      seg-alloc.bin.zst
      seg-change.bin.zst
  packs/
    pack-000001.pack
    pack-000001.pidx
    pack-000002.pack
    pack-000002.pidx
  indexes/
    active.json
    active.prev.json
    segments/
      idx-000001.seg
      idx-000002.seg
  gc/
    runs/
      gc-2026-02-24T02-00-00Z.json
  locks/
    repo.lock
  tmp/
```

## File Format Details

### `repo.json`

```json
{
  "repoVersion": 1,
  "repoUUID": "...",
  "chunkSizeBytes": 4194304,
  "chunkHash": "sha256",
  "packTargetSizeBytes": 536870912,
  "compression": "zstd",
  "createdAt": "2026-02-23T00:00:00Z"
}
```

### `pack-*.pack`

Append-only binary file.

- pack header (once)
  - magic: `RBPK`
  - version: `1`
  - pack ID
  - repo UUID
- repeated chunk records
  - tag: `0x01`
  - record length
  - chunk ID (32 bytes)
  - raw length (u32)
  - stored length (u32)
  - codec (`0=none`, `1=zstd`)
  - payload CRC32C
  - payload bytes
- optional end marker
  - tag: `0xFF`
  - record count

Behavior:
- never rewrite in-place
- only append new chunk records

### `pack-*.pidx`

Per-pack side index.

- magic: `RPIX`
- version: `1`
- pack ID
- entry count
- repeated entries (sorted by chunk ID)
  - chunk ID (32 bytes)
  - pack offset (u64)
  - stored length (u32)
  - raw length (u32)
  - codec (u8)

### `indexes/segments/idx-*.seg`

Global lookup segments for dedup and restore lookup.

Each entry:
- chunk ID
- pack ID
- pack offset
- stored length
- raw length
- codec

`indexes/active.json` contains ordered active segment list.

```json
{
  "version": 1,
  "segments": [
    "idx-000001.seg",
    "idx-000002.seg"
  ],
  "createdAt": "2026-02-24T01:02:03Z"
}
```

## Snapshot Manifest Format

Each generation has a snapshot directory with one header + binary segments.

### `manifest.json`

```json
{
  "manifestID": "2026-02-24T01-00-00Z",
  "parentManifestID": "2026-02-23T01-00-00Z",
  "policy": "daily-policy-a",
  "createdAt": "2026-02-24T01:00:12Z",
  "volume": {
    "sizeBytes": 107374182400,
    "chunkSizeBytes": 4194304
  },
  "cbt": {
    "mode": "delta",
    "baseSnapshotID": "...",
    "targetSnapshotID": "..."
  },
  "segments": {
    "alloc": "seg-alloc.bin.zst",
    "change": "seg-change.bin.zst"
  },
  "stats": {
    "changedChunks": 512,
    "newChunks": 221,
    "reusedChunks": 291,
    "bytesWritten": 927989760
  }
}
```

### `seg-alloc.bin.zst`

Allocated state segment.

- magic: `RBAL`
- version: `1`
- repeated range records
  - start chunk index (u64)
  - end chunk index exclusive (u64)

For base snapshots, this represents full allocated set.
For delta snapshots, this represents allocation delta (`added`/`removed` blocks with tags).

### `seg-change.bin.zst`

Changed chunk mapping segment.

- magic: `RBCH`
- version: `1`
- repeated records (sorted by chunk index)
  - chunk index (u64)
  - chunk ID (32 bytes)
  - state (u8: `1=allocated`, `2=freed`)

## Backup Write Path

1. get CBT ranges (`allocated` or `delta`)
2. normalize to chunk boundaries
3. read chunks from snapshot-mounted block device
4. compute chunk ID per chunk
5. lookup global index
6. if chunk exists, reuse reference only
7. if missing, append record to current pack
8. append index entries to new `idx-*.seg`
9. write snapshot segments and `manifest.json`
10. atomically update `refs/latest.txt`

## Commit and Crash Safety Order

Durability order (must be preserved):

1. append chunk payloads to `pack-*.pack`
2. fsync pack
3. persist `pack-*.pidx` update and new global `idx-*.seg`
4. fsync index files
5. write snapshot segments + `manifest.json`
6. fsync snapshot files
7. atomically replace `refs/latest.txt`

Invariant:
- manifest must never reference non-durable chunk locations

## Generation Management (Retention)

For `keepDays = N`:
- keep only successful snapshots newer than `now - N days`
- delete older snapshot directories in time order
- then run GC

`Failed` generation metadata can be retained for troubleshooting but is not part of live restore set.

## GC and Pack Compaction

v1 uses epoch mark-and-sweep.

1. build live chunk set by traversing retained snapshots
2. rebuild new active index set from live chunks
3. remove dead-only packs
4. compact mixed packs when fragmentation threshold reached
5. atomically swap `indexes/active.json` (`active.prev.json` kept for rollback)

Suggested compaction trigger:
- live ratio `< 0.7` or dead bytes `> 128 MiB` in a pack

## Restore Path

1. choose target snapshot manifest
2. resolve effective chunk map by applying parent chain
3. collect required chunk IDs
4. group reads by pack ID and offset order
5. read chunk payloads via index and write to destination chunk offsets
6. for unallocated chunks write zeros (or hole if backend supports sparse)

## Example: Changed Chunk In Incremental Backup

- generation A: `chunkIndex 420 -> chunk_old` stored in `pack-000002.pack`
- generation B: `chunkIndex 420` changed -> `chunk_new`
- if `chunk_new` not found in index, append it to `pack-000005.pack`
- B manifest contains only `chunkIndex 420 -> chunk_new`
- `chunk_old` remains until no retained snapshot references it

## Operational Defaults

- one writer per repository (`locks/repo.lock`)
- multiple readers allowed
- backup and restore can run during GC only if GC uses isolated new index generation and atomic swap

## Compatibility and Migration

- `repoVersion` controls decode path
- new optional fields may be added to `manifest.json`
- binary segment version bump required for incompatible changes
- chunking spec changes require new repository
