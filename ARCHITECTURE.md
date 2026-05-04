# Architecture Baseline

## Goal

Snaplane is a Kubernetes operator for block-volume backup and restore with:
- backup orchestration through `BackupPolicy` and `Backup`
- `VolumeSnapshot` as the consistency point
- changed-block input through CBT providers
- node-local backup storage under `/var/backup`
- restore through `PersistentVolumeClaim.dataSourceRef -> Backup`
- restore integration through `VolumePopulator`
- strict serial dispatch with no overtaking inside one policy

## Implemented Components

- Controller manager (`cmd/main.go`)
  - runs `BackupPolicy` and `Backup` controllers
- Writer sidecar (`cmd/writer-sidecar/main.go`)
  - runs on backup-target nodes
  - exposes mTLS gRPC write APIs
  - publishes node health and capacity through Lease annotations
- Restore populator (`cmd/restore-populator/main.go`)
  - registers `sourceKind=Backup`
  - restores either `mock-image-v1` or `cas-v1`

## API Surface

- `BackupPolicy`
  - source PVC and `VolumeSnapshotClass`
  - cron schedule with optional timezone and suspend flag
  - retention in days
  - transfer timeout and retry limit
  - manual trigger via `spec.manual.requestID`
- `Backup`
  - immutable references to the parent policy and `VolumeSnapshot`
  - immutable destination node
  - progress, retry, summary condition, and restore metadata in status

## Control Plane

### `BackupPolicy` controller

- creates deterministic `VolumeSnapshot` objects for scheduled and manual runs
- stores queue state on `VolumeSnapshot` metadata
- dispatches only the oldest non-`Done` snapshot
- blocks later work when the head snapshot is `Failed`
- resolves and pins the destination node on the source PVC
- creates one `Backup` object per dispatched snapshot
- applies retention by deleting old `Done` snapshots and their `Backup` objects, except when a restore still references them or a retained CAS backup still needs them as manifest-chain ancestors

### `Backup` controller

- waits for the referenced `VolumeSnapshot`
- streams data to the writer sidecar
- retries retryable failures with exponential backoff and jitter
- marks `Succeeded=True` only after restore metadata is durable
- publishes `status.restoreSource` for restore consumers
- publishes CAS manifest-chain ancestry in `status.restoreSource`

### Restore populator

- accepts only `Backup` objects with `Succeeded=True`
- reads `status.restoreSource`
- restores backup data into `pvcPrime`
- uses `lib-volume-populator` provider mode; Snaplane does not have a separate restore CRD

## Queue Model

Queue ownership lives on `VolumeSnapshot` metadata:
- label `snaplane.molpako.github.io/backup-state`
- label `snaplane.molpako.github.io/policy`
- annotation `snaplane.molpako.github.io/queueTime`
- annotation `snaplane.molpako.github.io/backupName`

Rules:
- queue order is `queueTime ASC`, then snapshot name
- only the oldest non-`Done` snapshot may dispatch
- `Failed` at the queue head blocks later snapshots
- manual retry is handled by creating a `Backup` for the failed snapshot

## Node Placement And Writer Plane

- eligible nodes require label `snaplane.molpako.github.io/backup-target=true`
- writer sidecars publish `writer-endpoint`, `used-bytes`, and `available-bytes` on a Lease
- candidate selection requires:
  - `Ready=True`
  - fresh Lease heartbeat
  - valid endpoint and capacity annotations
  - at least `10 GiB` available
- first assignment picks the lowest `used-bytes`, with `nodeName` as the tie-breaker
- the chosen node is pinned on the source PVC
- if the pinned node becomes invalid, the backup fails with `AssignedNodeUnavailable`
- v1 does not do automatic failover

## Data Formats

- default writer mode is `mock`
- optional `SNAPLANE_BACKUP_WRITE_MODE=cas` writes a PVC-scoped CAS repository under `/var/backup/<namespace>/<pvc>/repo`
- restore supports:
  - `mock-image-v1`
  - `cas-v1`

## Explicitly Out Of Scope

- automatic destination failover
- GC and compaction runtime execution
- a production-ready SnapshotMetadata-backed CBT data path
- a Ceph-backed nightly CBT gate; the current nightly lane remains a hostpath-based integration lane
- benchmark and performance tuning work

Those items stay in `TODO.md`.
