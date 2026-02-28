# Architecture Baseline

## Goal

Design a Kubernetes Operator for Ceph RBD backup and restore with:
- backup destination: node-local filesystem
- destination placement: `backup-target=true` node pool + PVC-level pinned node assignment
- change detection: SnapshotMetadata CBT ranges
- backup unit of work: `VolumeSnapshot` -> `Backup`
- restore entrypoint: `PersistentVolumeClaim.dataSourceRef -> Backup`
- restore execution: `VolumePopulator(sourceKind=Backup)` + CAS materialization
- transfer execution: serial only
- retention: N-day policy

## Architecture Boundaries

In scope:
- CRD/API model for backup and restore orchestration
- Reconcile flow for scheduled and manual backup dispatch
- Queueing/ordering model over `VolumeSnapshot`
- Retention policy and operational constraints
- Failure handling and observability requirements

Out of scope for this phase:
- implementation details in controller-runtime code
- production deployment manifests
- benchmark numbers and tuning scripts

## Evidence Anchors

Ceph-CSI exposes CBT via SnapshotMetadata service APIs:
- GetMetadataAllocated and GetMetadataDelta docs:
  - /Users/molpako/src/github.com/ceph/ceph-csi/docs/rbd/deploy.md:564
- Request/response semantics and stream constraints:
  - /Users/molpako/src/github.com/ceph/ceph-csi/vendor/github.com/container-storage-interface/spec/lib/go/csi/csi.pb.go:5902
- Current ceph-csi implementation and validation:
  - /Users/molpako/src/github.com/ceph/ceph-csi/internal/rbd/sms_controllerserver.go:82
  - /Users/molpako/src/github.com/ceph/ceph-csi/internal/rbd/snap_diff.go:33

Ceph feature dependencies relevant to CBT performance and behavior:
- fast-diff depends on object-map and exclusive-lock:
  - /Users/molpako/src/github.com/ceph/ceph/doc/rbd/rbd-config-ref.rst:123
  - /Users/molpako/src/github.com/ceph/ceph/doc/rbd/rbd-config-ref.rst:139

RBD diff stream record semantics (updated vs zero ranges):
- /Users/molpako/src/github.com/ceph/ceph/doc/dev/rbd-diff.rst:47

Snapshot metadata sidecar streaming contract references:
- /Users/molpako/src/github.com/kubernetes-csi/external-snapshot-metadata/proto/schema.proto:64
- /Users/molpako/src/github.com/kubernetes-csi/external-snapshot-metadata/pkg/sidecar/sidecar.go:43

## Operator Control Plane Sketch

v1alpha1 resources:
- BackupPolicy
- Backup
- VolumeSnapshot (existing)
- PersistentVolumeClaim (existing, restore request entrypoint)
- VolumePopulator (existing CRD, registration for `sourceKind=Backup`)

CRD draft details:
- /Users/molpako/src/github.com/molpako/rbd-local-backup/docs/design-docs/crd-design.md
- /Users/molpako/src/github.com/molpako/rbd-local-backup/docs/design-docs/restore-volume-populator-contract.md

Controller topology (A-2, locked):
- `BackupPolicy` controller (single queue writer)
  - watches `BackupPolicy` (primary)
  - watches `VolumeSnapshot` (secondary trigger for `readyToUse` transition)
  - watches `Backup` (secondary trigger for terminal transition)
- `Backup` controller (execution only)
  - watches `Backup` (primary)
- restore populator controller is external to operator ownership and is not counted in A-2.

Reconcile model:
- `BackupPolicy` controller creates deterministic `VolumeSnapshot` at scheduled time.
- `VolumeSnapshot` queue is carried by label/annotation (`Pending`,`queueTime`,`policy`,`backupName`).
- immediate run is triggered only by `BackupPolicy` annotation `reconcile.snaplane.molpako.github.io/requestedAt` (`RFC3339Nano UTC`); invalid value is surfaced as `QueueHealthy=False`.
- `BackupPolicy` controller dispatches by queue order with CAS (`Pending -> Dispatched`) and create-or-get `Backup`.
- `BackupPolicy` controller resolves `Backup.spec.destination.nodeName` from backup-target node candidates and PVC assignment annotations.
- `Backup` controller transfers data from referenced snapshot and updates `Backup.status.conditions` (conditions-first model).
- `Backup` controller publishes restore metadata (`status.restoreSource.*`) only when backup data is durable and `Completed=True` can be set.
- `Backup` controller resolves writer endpoint from node Lease annotations and executes writer gRPC (`StartWrite` -> `WriteFrames` -> `CommitWrite`).
- `BackupPolicy` controller observes `Backup` terminal state and updates snapshot queue state to `Done`/`Failed`.
- restore populator controller watches PVC `dataSourceRef`, accepts only `Completed` Backup, restores CAS data into target PVC through lib-volume-populator `pvcPrime` flow.
- retention loop skips deleting Backup objects that are currently referenced by active (unbound / finalizer-attached) restore PVCs.
- If the oldest non-`Done` snapshot is `Failed`, dispatch stops until that snapshot is retried manually and completed.

Node placement and writer plane (v1 locked):
- candidate nodes must satisfy:
  - node label `snaplane.molpako.github.io/backup-target=true`
  - `Ready=True`
  - fresh Lease heartbeat (<= 90s)
  - `availableBytes >= 10GiB`
- first assignment is `usedBytes` minimum (tie-breaker: nodeName ASC), then pinned on PVC annotations.
- if assigned node is unavailable, `Backup` fails with `AssignedNodeUnavailable`; automatic failover is not enabled in v1.
- writer sidecar runs as DaemonSet on backup-target nodes and writes to hostPath `/var/backup`.
- controller <-> writer connection is mTLS-only.

## Data Plane Sketch

backup path:
- create `VolumeSnapshot` as consistency point
- wait for `VolumeSnapshot.status.readyToUse=true`
- collect metadata ranges using SnapshotMetadata APIs
- selective block read and transfer via writer gRPC frame stream (`DATA`/`ZERO`/`EOF`)
- mark completion on `Backup` and snapshot queue labels

restore path:
- user creates PVC with `spec.dataSourceRef={apiGroup:snaplane...,kind:Backup,name:...}`
- populator validates `Backup.status.conditions` (`Completed=True`, `Failed!=True`) and `status.restoreSource`
- populator resolves CAS manifest chain and chunk map from repository
- populator writes chunk payloads to target PVC (`DATA` + zero-fill for unallocated ranges)
- populator completes PV rebind and releases temporary resources

## Reliability Principles

- idempotent naming and create-or-get behavior
- serial dispatch by queue order (no overtaking)
- CAS update for `Pending -> Dispatched`
- deterministic recovery from partial reconcile
- failed-head blocking until manual retry succeeds

## Next Design Focus

- end-to-end rollout and operational runbook for writer sidecar + cert-manager
- restore worker rollout/runbook and failure observability for populator events
- backup execution retry envelope and resume semantics beyond mock writer
- retention deletion timing with active-restore protection
