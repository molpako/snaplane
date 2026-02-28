# Backup Data Model

## Terms

- chunk
  - fixed-size logical range unit over volume (recommended baseline 4 MiB)
- chunk ID
  - content hash of one chunk payload
- chunk index
  - zero-based logical position of a chunk in the source volume
- pack
  - append-only file containing many chunks
- index
  - mapping from chunk ID to pack location
- manifest
  - generation metadata mapping chunk allocation state and chunk references

## Repository Layout

- repo.json
  - immutable chunking spec (chunk size, hash, pack target size, version)
- packs/
  - pack-N.pack and optional side index for fast lookup
- snapshots/
  - manifest files (base and delta)
- tmp/
  - temporary write files
- locks/
  - advisory locks and ownership metadata

## Base Backup

Input:
- target snapshot A

Flow:
- fetch allocated ranges for A
- normalize ranges to chunk boundaries
- read only allocated chunks from device
- hash and dedup by chunk ID
- append missing chunks to pack and update index
- write base manifest A

## Incremental Backup

Input:
- base snapshot A
- target snapshot B

Flow:
- fetch delta ranges A->B
- normalize ranges to chunk boundaries
- read only candidate chunks
- hash and dedup by chunk ID
- append missing chunks and update index
- write delta manifest B with parent=A

## Restore

- resolve generation chain from target back to base
- compute final chunk index -> chunk ID mapping
- materialize chunks into destination blocks
- write zeros or sparse holes for unallocated chunks

## Retention

- keep N days of successful generations
- delete oldest generations beyond policy
- run GC to reclaim unreferenced chunks

## Invariants

- repo chunking spec immutable after repository creation
- manifest commit must reference only durable chunk locations
- index updates and pack writes are ordered for crash recovery

## See Also

- storage-format-v1.md
  - concrete on-disk format and binary record contracts
