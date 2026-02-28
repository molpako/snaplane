# AGENTS Index

This file is an index for agent context in this repository.
Do not store deep design details here.
Store details in linked files.

## Project Scope

Design a Kubernetes Operator for Ceph RBD local backup.
Current locked direction:
- `BackupPolicy` + `Backup` CRDs
- existing `VolumeSnapshot` as consistency point
- metadata queue on `VolumeSnapshot`
- strict serial dispatch and no overtake
- node-local filesystem backup repository model

## Read Order For Agents

1. /Users/molpako/src/github.com/molpako/rbd-local-backup/ARCHITECTURE.md
2. /Users/molpako/src/github.com/molpako/rbd-local-backup/docs/design-docs/index.md
3. /Users/molpako/src/github.com/molpako/rbd-local-backup/docs/design-docs/core-beliefs.md
4. /Users/molpako/src/github.com/molpako/rbd-local-backup/docs/design-docs/operator-architecture.md
5. /Users/molpako/src/github.com/molpako/rbd-local-backup/docs/design-docs/restore-volume-populator-contract.md
6. /Users/molpako/src/github.com/molpako/rbd-local-backup/docs/design-docs/backup-target-node-writer-contract.md
7. /Users/molpako/src/github.com/molpako/rbd-local-backup/docs/design-docs/crd-design.md
8. /Users/molpako/src/github.com/molpako/rbd-local-backup/docs/design-docs/crd-schema-draft.md
9. /Users/molpako/src/github.com/molpako/rbd-local-backup/docs/design-docs/controller-reconcile-contract.md
10. /Users/molpako/src/github.com/molpako/rbd-local-backup/docs/design-docs/backup-data-model.md
11. /Users/molpako/src/github.com/molpako/rbd-local-backup/docs/design-docs/storage-format-v1.md
12. /Users/molpako/src/github.com/molpako/rbd-local-backup/docs/design-docs/manifest-format-options.md
13. /Users/molpako/src/github.com/molpako/rbd-local-backup/docs/design-docs/gc-strategy.md
14. /Users/molpako/src/github.com/molpako/rbd-local-backup/docs/design-docs/requirements-gap-analysis.md
15. /Users/molpako/src/github.com/molpako/rbd-local-backup/docs/exec-plans/active/2026-02-23-v1alpha1-api-controller-implementation.md

## Source Of Truth Map

- Architecture baseline:
  - /Users/molpako/src/github.com/molpako/rbd-local-backup/ARCHITECTURE.md
- Stable design decisions:
  - /Users/molpako/src/github.com/molpako/rbd-local-backup/docs/design-docs/
- Open execution work:
  - /Users/molpako/src/github.com/molpako/rbd-local-backup/docs/exec-plans/active/
- Completed work logs:
  - /Users/molpako/src/github.com/molpako/rbd-local-backup/docs/exec-plans/completed/
- Known debt tracker:
  - /Users/molpako/src/github.com/molpako/rbd-local-backup/docs/exec-plans/tech-debt-tracker.md
- Product behavior contracts:
  - /Users/molpako/src/github.com/molpako/rbd-local-backup/docs/product-specs/
- Generated artifacts:
  - /Users/molpako/src/github.com/molpako/rbd-local-backup/docs/generated/
- External reference notes:
  - /Users/molpako/src/github.com/molpako/rbd-local-backup/docs/references/

## Repository Context Index

- /Users/molpako/src/github.com/molpako/rbd-local-backup/AGENTS.md: index only
- /Users/molpako/src/github.com/molpako/rbd-local-backup/ARCHITECTURE.md: architecture baseline and boundaries
- /Users/molpako/src/github.com/molpako/rbd-local-backup/docs/design-docs/index.md: design entry point
- /Users/molpako/src/github.com/molpako/rbd-local-backup/docs/design-docs/core-beliefs.md: engineering principles
- /Users/molpako/src/github.com/molpako/rbd-local-backup/docs/design-docs/operator-architecture.md: control plane and reconcile model
- /Users/molpako/src/github.com/molpako/rbd-local-backup/docs/design-docs/restore-volume-populator-contract.md: restore entry (`PVC.dataSourceRef -> Backup`) and populator acceptance contract
- /Users/molpako/src/github.com/molpako/rbd-local-backup/docs/design-docs/backup-target-node-writer-contract.md: node placement, writer gRPC, mTLS contract
- /Users/molpako/src/github.com/molpako/rbd-local-backup/docs/design-docs/crd-design.md: locked CRD contracts
- /Users/molpako/src/github.com/molpako/rbd-local-backup/docs/design-docs/crd-schema-draft.md: kubebuilder schema draft
- /Users/molpako/src/github.com/molpako/rbd-local-backup/docs/design-docs/controller-reconcile-contract.md: reconcile ownership and invariants
- /Users/molpako/src/github.com/molpako/rbd-local-backup/docs/design-docs/backup-data-model.md: chunk/chunk-index/pack/index model
- /Users/molpako/src/github.com/molpako/rbd-local-backup/docs/design-docs/storage-format-v1.md: concrete repository on-disk format (pack/index/manifest)
- /Users/molpako/src/github.com/molpako/rbd-local-backup/docs/design-docs/manifest-format-options.md: manifest options
- /Users/molpako/src/github.com/molpako/rbd-local-backup/docs/design-docs/gc-strategy.md: GC and compaction policy
- /Users/molpako/src/github.com/molpako/rbd-local-backup/docs/design-docs/requirements-gap-analysis.md: requirement gaps

## External Evidence Paths (Local)

Use local repositories for fact checks:
- /Users/molpako/src/github.com/ceph/ceph
- /Users/molpako/src/github.com/ceph/ceph-csi
- /Users/molpako/src/github.com/csi-addons/spec
- /Users/molpako/src/github.com/csi-addons/kubernetes-csi-addons

## Working Rules For Agents

- Keep `AGENTS.md` around 100 lines and index-only.
- Add design details under `docs/design-docs/`.
- Update `docs/design-docs/index.md` whenever design docs change.
- Reflect major design changes in `ARCHITECTURE.md`.
- Keep unresolved decisions in `docs/exec-plans/active/`.

## Out Of Scope Right Now

- Runtime/controller implementation code
- Cluster deployment and operations
- Performance benchmarking implementation
