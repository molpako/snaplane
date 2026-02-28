# AGENTS Index

This file is an index for repository context.
Keep it short. Put detailed design text in linked documents.

## Project Scope

Snaplane is a Kubernetes operator for block-volume backup and restore with:
- `BackupPolicy` and `Backup` CRDs
- `VolumeSnapshot` as the backup consistency point
- changed-block input through CBT providers
- strict serial dispatch with no overtaking
- node-local backup repositories managed by a writer sidecar
- restore through `PersistentVolumeClaim.dataSourceRef -> Backup`
- restore integration through `VolumePopulator`

## Read Order

1. ARCHITECTURE.md
2. TODO.md
3. docs/design-docs/index.md
4. docs/design-docs/core-beliefs.md
5. docs/design-docs/operator-architecture.md
6. docs/design-docs/crd-design.md
7. docs/design-docs/controller-reconcile-contract.md
8. docs/design-docs/backup-target-node-writer-contract.md
9. docs/design-docs/restore-volume-populator-contract.md
10. docs/design-docs/backup-data-model.md
11. docs/design-docs/storage-format-v1.md
12. docs/design-docs/volumereplication-non-adoption.md

## Source Of Truth

- Current architecture: ARCHITECTURE.md
- Current implementation: api and internal
- Stable design notes: docs/design-docs
- Open work only: TODO.md


## Working Rules

- Keep `AGENTS.md` index-only.
- Update `ARCHITECTURE.md` when the implemented architecture changes.
- Update `docs/design-docs/index.md` when retained design docs change.
- Put not-yet-implemented work in `TODO.md`, not in scattered draft docs.
- Prefer real integrations in e2e coverage unless a temporary exception is explicitly documented.
