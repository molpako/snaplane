# Backup Target Node And Writer Contract

## Purpose

This document describes the implemented contract for choosing a node-local backup target and writing data through the writer sidecar.

## Locked Rules

1. Only nodes with label `snaplane.molpako.github.io/backup-target=true` are eligible.
2. Assignment is pinned per source PVC through PVC annotations.
3. First assignment chooses the candidate with the lowest `used-bytes`, then `nodeName`.
4. Runtime node state is published through `coordination.k8s.io/v1 Lease`, not a new CRD.
5. If the assigned node becomes unusable, the backup fails; v1 does not fail over automatically.
6. Controller-to-writer traffic is mTLS only.
7. Writer RPCs are `StartWrite`, `WriteFrames`, `CommitWrite`, and `AbortWrite`.

## Metadata

### Node label

- `snaplane.molpako.github.io/backup-target=true`

### PVC annotations

- `snaplane.molpako.github.io/assigned-backup-node=<nodeName>`
- `snaplane.molpako.github.io/assigned-backup-node-at=<RFC3339>`

### Lease annotations

- `snaplane.molpako.github.io/writer-endpoint=<podIP:port>`
- `snaplane.molpako.github.io/used-bytes=<int64>`
- `snaplane.molpako.github.io/available-bytes=<int64>`
- `snaplane.molpako.github.io/cas-maintenance-last-run=<RFC3339>`
- `snaplane.molpako.github.io/cas-maintenance-last-error=<string>`
- `snaplane.molpako.github.io/cas-maintenance-repos=<int>`
- `snaplane.molpako.github.io/cas-maintenance-reclaimed-bytes=<int64>`

## Candidate Selection

A node is a valid candidate only when all of these are true:
- the backup-target label is present
- `NodeReady=True`
- the writer Lease exists and is fresh
- the Lease exposes endpoint and capacity annotations
- `available-bytes >= 10 GiB`

Selection behavior:
- if the PVC has no pinned node, choose the best current candidate and patch the PVC
- if the PVC already has a pinned node and it is still a valid candidate, reuse it
- if the pinned node is no longer valid, set `Backup.spec.destination.nodeName` to `unavailable`
- if no candidate is valid, also use `unavailable`

## Backup API Impact

- `Backup.spec.destination.nodeName` is required and immutable
- execution fails with `AssignedNodeUnavailable` when the destination is `unavailable` or the resolved writer cannot be reached

## Writer Protocol

### RPCs

- `StartWrite` is idempotent by `backup_uid`
- `WriteFrames` is client streaming
- `CommitWrite` finalizes a session after EOF
- `AbortWrite` best-effort cleans up an incomplete session

### Frame types

- `DATA(offset, payload, crc32c)`
- `ZERO(offset, length)`
- `EOF(logical_size)`

### Session rules

- offsets must be contiguous
- empty data frames are rejected
- CRC mismatch is rejected
- `CommitWrite` requires EOF

## Writer Storage Modes

### Mock mode

- writes to `/var/backup/<namespace>/<pvc>/<backup>/`
- persists `mock.img` and `meta.json`

### CAS mode

- writes to `/var/backup/<namespace>/<pvc>/repo`
- publishes repository metadata used later by restore
- writer-sidecar maintenance discovers repositories at that layout and runs CAS compaction on a fixed interval
- the interval is controlled by `-cas-maintenance-interval`; `0` disables maintenance
- the latest maintenance result is exported through the writer Lease annotations

## Operational Defaults

- heartbeat interval: `30s`
- stale threshold: `90s`
- minimum available bytes: `10 GiB`
- automatic failover: disabled
