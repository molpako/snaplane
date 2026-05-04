# Why Snaplane Does Not Use CSI-Addons VolumeReplication As Its Backup Policy API

## Decision

Snaplane v1 keeps `BackupPolicy` as the backup orchestration source of truth.
`VolumeReplication` is not adopted as the parent backup policy object.

## Reasoning

- Snaplane manages backup generations and restore points outside the source CSI backend.
- Snaplane needs cron scheduling, retention, queue ordering, and restore metadata publication.
- `VolumeReplication` is designed around DR replication state, not backup archive lifecycle.
- Replacing `BackupPolicy` with `VolumeReplication` would still require Snaplane-specific policy annotations and controller logic.

## Practical Outcome

- keep `BackupPolicy` for scheduling and retention
- keep `Backup` for transfer execution and restore metadata
- keep `VolumeSnapshot` metadata as the queue
- allow future integration with `VolumeReplication` as an input signal if needed, but not as the primary API
