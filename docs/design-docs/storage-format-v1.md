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
- writer-sidecar CAS maintenance runs repository compaction
- dependency-safe manifest pruning with parent-chain closure
- repository, active index, change segment, index entry, and pack payload integrity checks during commit, compaction, and restore

Not implemented:
- compression beyond `codec=none`

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

Compatibility rules:
- readers and writers accept only `repoVersion=1`
- `chunkHash` must be absent or `sha256`
- `compression` must be absent or `none`
- unsupported future versions or codecs fail closed instead of being interpreted as v1

### `packs/pack-*.pack`

- append-only raw chunk payload file
- current implementation stores payload bytes directly with no extra framing
- restore and compaction verify `SHA-256(raw payload) == chunkID` before trusting a referenced payload

### `packs/pack-*.pidx`

- JSON side index for one pack
- stores chunk ID, offset, lengths, and codec

### `indexes/segments/idx-*.seg`

- JSON list of newly published global index entries
- `indexes/active.json` lists the active segment files in order
- `indexes/active.json` must have `version=1`
- index entries must have a non-empty chunk ID, positive pack ID, non-negative offset, positive stored/raw lengths, and `codec=none`

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
- `allocated` means the logical chunk position resolves to the specified `chunkID`
- `freed` means the logical chunk position resolves to zero-fill and must not carry a `chunkID`
- backup generation emits `freed` when a changed chunk's current payload is all zero bytes

### `refs/latest.txt`

- latest published manifest ID

## Restore Semantics

- restore walks the manifest parent chain from the requested manifest backward
- later change records override earlier ones for the same chunk index
- `freed` resolves to zero-fill on the target device
- chunk payloads are read from packs through the active global index
- unknown change states, missing chunk locations, unsupported codecs, and checksum mismatches fail restore

## Durability Model

- repository publication is guarded by `locks/repo.lock`
- pack writes are synced before index and manifest publication
- the latest ref is updated only after manifest publication succeeds
- compaction holds the same repository lock while rewriting active index and pack files
- manifest pruning holds the same repository lock while deleting unkept manifest directories, updating `refs/latest.txt`, and compacting packs/indexes

## Pruning Semantics

- pruning input is an explicit list of manifest IDs to keep
- each kept manifest is expanded to its full parent chain before deletion
- only manifest directories outside that keep closure are deleted
- `refs/latest.txt` is moved to the newest kept manifest, or removed when no manifests are kept
- compaction runs under the same lock after manifest deletion so unreferenced chunks are removed without racing backup publish

## Open Questions

Future changes belong in `TODO.md`, not in this document.
