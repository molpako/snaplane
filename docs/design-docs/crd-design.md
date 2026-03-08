# CRD Design

## Purpose

This document summarizes the API shape that is implemented in `api/v1alpha1`.

## Kinds

- `BackupPolicy`
- `Backup`

Related external kinds:
- `VolumeSnapshot`
- `PersistentVolumeClaim`
- `VolumePopulator`

Snaplane does not define a `Restore` CRD or a `BackupContent` CRD in v1alpha1.

## `BackupPolicy`

`BackupPolicy.spec` contains:
- source PVC name
- `VolumeSnapshotClass` name
- cron schedule
- optional timezone
- optional suspend flag
- serial dispatch mode
- retention in days
- transfer timeout and backoff limit
- manual request ID

`BackupPolicy.status` contains:
- conditions
- last scheduled time
- last dispatched snapshot
- active backup name
- pending snapshot count
- manual request acknowledgement

## `Backup`

`Backup.spec` contains:
- immutable policy reference
- immutable `VolumeSnapshot` reference
- immutable destination node
- immutable transfer settings

`Backup.status` contains:
- summary condition `Succeeded`
- start and completion timestamps
- last error
- transfer stats
- progress counters
- retry state
- restore source metadata

## Queue Contract

Queue state is stored on `VolumeSnapshot` metadata, not on a dedicated queue CRD.

States:
- `Pending`
- `Dispatched`
- `Done`
- `Failed`

Rules:
- queue ownership belongs to the `BackupPolicy` controller
- dispatch is strict serial
- a failed queue head blocks later snapshots
- restore retention protection is based on active PVC restore references
