# Repository Storage Format v1

## Purpose

This document describes the repository layout currently used by Snaplane CAS mode.

## Current Status

Implemented:
- CAS repository publish for backup writes
- CAS restore materialization from manifests, index segments, and pack files
- fixed-size chunking with `4 MiB` chunks
- chunk IDs as `SHA-256(raw chunk bytes)`
- PVC-scoped repositories under `/var/backup/<namespace>/<pvc>/repo`
- append-only pack files and append-only global index segments
- manifest parent chaining for generation history
- manifest-chain ancestry publication in `Backup.status.restoreSource`
- explicit `allocated` and `freed` chunk states in change records

Not implemented:
- GC and compaction runtime execution
- compression beyond `codec=none`
- a production-ready SnapshotMetadata-backed CBT data source

## Repository Tree

```text
repo/
  repo.json
  refs/
    latest.txt
  snapshots/
    <manifest-id>/
      manifest.json
      seg-alloc.bin.zst
      seg-change.bin.zst
  packs/
    pack-000001.pack
    pack-000001.pidx
  indexes/
    active.json
    segments/
      idx-000001.seg
  locks/
    repo.lock
  tmp/
```

## Core Files

### `repo.json`

Defines immutable repository-wide settings:
- `repoVersion`
- `repoUUID`
- `chunkSizeBytes`
- `chunkHash`
- `packTargetSizeBytes`
- `compression`

### `packs/pack-*.pack`

- append-only raw chunk payload file
- current implementation stores payload bytes directly with no extra framing

### `packs/pack-*.pidx`

- JSON side index for one pack
- stores chunk ID, offset, lengths, and codec

### `indexes/segments/idx-*.seg`

- JSON list of newly published global index entries
- `indexes/active.json` lists the active segment files in order

### `snapshots/<manifest>/manifest.json`

Stores:
- `manifestID`
- `parentManifestID`
- policy name
- creation time
- volume size and chunk size
- segment filenames
- changed/new/reused chunk counters

### `snapshots/<manifest>/seg-change.bin.zst`

- JSON change records
- each record uses:
  - `chunkIndex`
  - `chunkID` for allocated chunks
  - `state=allocated|freed`

### `refs/latest.txt`

- latest published manifest ID

## Restore Semantics

- restore walks the manifest parent chain from the requested manifest backward
- later change records override earlier ones for the same chunk index
- `freed` resolves to zero-fill on the target device
- chunk payloads are read from packs through the active global index

## Durability Model

- repository publication is guarded by `locks/repo.lock`
- pack writes are synced before index and manifest publication
- the latest ref is updated only after manifest publication succeeds

## Open Questions

Future changes belong in `TODO.md`, not in this document.
