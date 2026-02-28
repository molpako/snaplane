# Restore VolumePopulator Contract

## Purpose

Snaplane restore is driven by `PersistentVolumeClaim.dataSourceRef -> Backup`. There is no dedicated restore CRD.

## Locked Rules

1. The restore entrypoint is PVC `dataSourceRef`, not a Snaplane restore object.
2. `dataSourceRef` must point to `apiGroup=snaplane.molpako.github.io`, `kind=Backup`.
3. The registered `VolumePopulator` uses `sourceKind=Backup`.
4. Restore accepts only backups with `Succeeded=True`.
5. Restore requires `Backup.status.restoreSource` to be complete enough to locate the source payload.
6. Cross-namespace restore is disabled by default.
7. Retention must not delete a backup while an active restore PVC still references it.

## Required Restore Metadata

The restore populator reads:
- `status.restoreSource.nodeName`
- `status.restoreSource.repositoryPath`
- `status.restoreSource.manifestID`
- `status.restoreSource.repoUUID`
- `status.restoreSource.format`
- optional size and chunk-size hints

Accepted formats:
- `mock-image-v1`
- `cas-v1`
- empty format is still accepted for backward compatibility

## Execution Model

- controller mode uses `lib-volume-populator` provider hooks
- worker mode restores one backup into the target device
- mock mode copies a single image file
- CAS mode reconstructs chunk state from manifest ancestry and repository indexes

## Retention Protection

Retention must skip a `Backup` when a PVC in the same namespace still references it through `dataSourceRef` and either:
- the PVC is not yet bound, or
- the populator finalizer is still present
