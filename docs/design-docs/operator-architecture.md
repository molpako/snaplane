# Operator Architecture

## Main Components

- controller manager
  - `BackupPolicy` controller
  - `Backup` controller
- writer sidecar DaemonSet on backup-target nodes
- restore populator controller and worker pods

## Backup Flow

1. `BackupPolicy` creates or gets a deterministic `VolumeSnapshot`.
2. The controller records queue metadata on that snapshot.
3. When the oldest pending snapshot is `readyToUse`, the controller resolves a destination node and creates a `Backup`.
4. The `Backup` controller opens a writer session, streams frames, and commits the result.
5. On success, the `Backup` controller publishes restore metadata and marks `Succeeded=True`.
6. The `BackupPolicy` controller marks the snapshot `Done`.

## Restore Flow

1. A user creates a PVC with `dataSourceRef` pointing to a `Backup`.
2. The restore populator validates that the backup succeeded and has restore metadata.
3. A worker restores backup data into `pvcPrime`.
4. `lib-volume-populator` rebinds the final PVC.

## Scheduling And Serial Guard

- queue order is based on `queueTime`
- later snapshots never overtake earlier ones
- only one non-terminal `Backup` is allowed per policy
- a failed head snapshot blocks later dispatches

## Node Assignment

- the source PVC stores the pinned backup node
- the first assignment comes from Lease-based capacity selection
- the pinned node is reused while it remains healthy
- unhealthy pinned nodes do not trigger reassignment in v1; they cause backup failure instead

## Storage Modes

- `mock` mode writes a single image per backup
- `cas` mode writes a shared repository per PVC and publishes manifest-based restore metadata
