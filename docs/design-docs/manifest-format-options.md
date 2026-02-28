# Manifest Format Options

Status:
- option study document
- v1 concrete format decision is documented in `storage-format-v1.md`

## Problem

Manifest payload can grow large for big volumes and daily chains.
A naive JSON map of chunkIndex->chunkID is too large and expensive to parse.

## Constraints

- fast append for incremental backups
- fast chain merge for restore
- bounded memory for controller pods
- corruption detection and versioned evolution

## Option A: Plain JSON Ranges

Description:
- store allocated ranges and changedChunks as JSON arrays

Pros:
- easy to inspect and debug

Cons:
- large payload size
- slow parse for long chains
- high memory overhead

## Option B: Full Bitmap + Delta Records (Binary)

Description:
- allocated bitmap compressed (zstd)
- changedChunks encoded as sorted chunk indexes + chunk IDs in binary blocks

Pros:
- compact and predictable
- fast scan/merge with low overhead

Cons:
- needs tooling for debugging
- versioned decoder required

## Option C: Hybrid Header + Binary Segments (Recommended v1)

Description:
- small JSON/TOML header for metadata and checksums
- binary segments for large arrays:
  - allocated representation
  - changed chunk records
  - optional freed chunk records

Why selected:
- operationally debuggable header
- compact bulk data
- easy forward-compat by segment versioning

## v1 Segment Proposal

- header:
  - manifest_id, parent_manifest_id
  - volume_size, chunk_size, created_at
  - segment checksums and codec
- seg_alloc:
  - base: full bitmap or range-set
  - delta: add/remove range sets
- seg_changes:
  - repeated records of chunk_index + chunk_id
- seg_meta:
  - counters and optional hints

## Open Items

- v1 decisions moved to `storage-format-v1.md`
- this file is retained as design rationale history
