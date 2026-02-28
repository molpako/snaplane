# Controller Reconcile Contract

## Purpose

This document defines the current responsibility split between the `BackupPolicy` and `Backup` controllers.

## Shared Invariants

1. Snaplane owns exactly two controllers in the manager: `BackupPolicy` and `Backup`.
2. Restore is handled by the external VolumePopulator-based controller.
3. Queue ownership belongs to the `BackupPolicy` controller.
4. Dispatch is strict-serial inside one policy.
5. Only the oldest non-`Done` snapshot is eligible for dispatch.
6. A failed queue head blocks later work.
7. `Pending -> Dispatched` is protected by a compare-and-patch transition on snapshot metadata.
8. Controllers do not mutate `spec` after object creation.
9. Status is patched only when it changes.

## Queue Metadata

- label `snaplane.molpako.github.io/backup-state`
- label `snaplane.molpako.github.io/policy`
- annotation `snaplane.molpako.github.io/queueTime`
- annotation `snaplane.molpako.github.io/backupName`

## `BackupPolicy` Controller

### Responsibilities

- create deterministic scheduled snapshots
- create deterministic manual snapshots from `spec.manual.requestID`
- repair missing queue metadata on owned snapshots
- dispatch the oldest eligible snapshot
- create or get the matching `Backup`
- observe terminal `Backup` state and mirror it back to the snapshot queue
- apply retention

### Dispatch gates

Dispatch stops when any of these are true:
- the oldest non-`Done` snapshot is `Failed`
- the oldest non-`Done` snapshot is already `Dispatched`
- the oldest non-`Done` snapshot is not `readyToUse`
- another `Backup` in the same policy is still non-terminal

### Manual behavior

- `spec.manual.requestID` is treated as pending until it is acknowledged in status
- a manual snapshot is only created when the policy has no non-`Done` snapshots
- manual retry of a failed head snapshot is done by creating a `Backup` for that snapshot

### Retention behavior

- only `Done` snapshots are candidates
- deletion is ordered by `queueTime`
- the paired `Backup` is deleted with the snapshot
- deletion is skipped when an active restore PVC still references that `Backup`

## `Backup` Controller

### Responsibilities

- resolve the referenced snapshot
- resolve the writer endpoint from the destination node Lease
- run writer sessions and stream frames
- retry retryable failures using `status.retry.*`
- publish `status.progress`, `status.stats`, and `status.restoreSource`
- set the summary condition `Succeeded`

### Execution model

- non-terminal work is represented as `Succeeded=Unknown`
- terminal success is `Succeeded=True`
- terminal failure is `Succeeded=False`
- timeout and endpoint errors may retry until the policy backoff limit is exhausted
- queue metadata is never updated from this controller

## Restore Contract Edge

- restore starts from PVC `dataSourceRef -> Backup`
- only `Succeeded=True` backups are accepted
- restore requires `status.restoreSource.repositoryPath` and `manifestID`
