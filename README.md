# Snaplane

Snaplane is a Kubernetes operator for block-volume backup and restore.

## Overview

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

## Current Status

Implemented:
- v1alpha1 `BackupPolicy` and `Backup` APIs
- strict serial dispatch with queue metadata on `VolumeSnapshot`
- destination node pinning on source PVCs
- writer-sidecar mTLS path
- retryable backup execution
- restore populator flow
- CAS repository publish and restore
- CAS compaction execution in writer-sidecar maintenance

Not yet implemented:
- production-ready SnapshotMetadata-backed CBT data path
- dependency-safe CAS repository pruning and GC fencing
- full operational rollout documentation

Track open work in [TODO.md](./TODO.md).

## Quick References

- architecture baseline: [ARCHITECTURE.md](./ARCHITECTURE.md)
- retained design docs: [docs/design-docs/index.md](./docs/design-docs/index.md)
- open work: [TODO.md](./TODO.md)
- e2e lanes: [test/e2e/README.md](./test/e2e/README.md)
