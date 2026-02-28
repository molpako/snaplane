# Snaplane

Snaplane is a Kubernetes operator for block-volume backup and restore.

Current implemented direction:
- `BackupPolicy` schedules and dispatches backups
- `Backup` represents one transfer execution
- `VolumeSnapshot` is the consistency point and queue carrier
- changed-block input is modeled through CBT providers
- writer sidecars store backup data on node-local storage under `/var/backup`
- restore is requested through `PersistentVolumeClaim.dataSourceRef -> Backup`
- restore integration depends on `VolumePopulator`

## Components

- controller manager
  - runs the `BackupPolicy` and `Backup` controllers
- writer sidecar
  - runs on backup-target nodes
  - exposes mTLS gRPC write APIs
  - reports endpoint and capacity through Lease annotations
- restore populator
  - registers `sourceKind=Backup`
  - restores `mock-image-v1` and `cas-v1` backups

## Repository Layout

- `api/`
  - v1alpha1 API types
- `cmd/`
  - binaries for the controller manager, writer sidecar, and restore populator
- `config/`
  - CRDs, RBAC, manager manifests, writer-sidecar manifests, and restore-populator manifests
- `internal/`
  - controller logic, writer server, restore worker, and CAS repository code
- `test/e2e/`
  - end-to-end coverage and lane documentation
- `docs/design-docs/`
  - retained design documents that still match the codebase

## Current Status

Implemented:
- v1alpha1 `BackupPolicy` and `Backup` APIs
- strict serial dispatch with queue metadata on `VolumeSnapshot`
- destination node pinning on source PVCs
- writer-sidecar mTLS path
- retryable backup execution
- restore populator flow
- CAS repository publish and restore

Not yet implemented:
- production-ready SnapshotMetadata-backed CBT data path
- CAS GC and compaction execution
- full operational rollout documentation

Track open work in [TODO.md](./TODO.md).

## Quick References

- architecture baseline: [ARCHITECTURE.md](./ARCHITECTURE.md)
- retained design docs: [docs/design-docs/index.md](./docs/design-docs/index.md)
- open work: [TODO.md](./TODO.md)
- e2e lanes: [test/e2e/README.md](./test/e2e/README.md)
