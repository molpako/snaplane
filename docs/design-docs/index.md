# Design Docs Index

This folder stores stable design decisions for the RBD local backup operator.

## Latest Updates

- 2026-03-01: restore を `Restore CRD` ではなく `PVC.dataSourceRef -> Backup` + `VolumePopulator(sourceKind=Backup)` で固定。`Completed` Backup のみ受理し CAS restore を実行する契約と retention 保護ルールを追加。
- 2026-02-28: scheduler contract aligned so assigned PVC node is reused only while candidate-healthy; otherwise destination is `unavailable` and transfer fails with `AssignedNodeUnavailable`.
- 2026-02-28: backup-target node placement and writer sidecar contract locked (PVC node pinning, Lease-based capacity scheduler, gRPC frame protocol, mTLS required).
- 2026-02-23: requestedAt immediate trigger contract tightened (`RFC3339Nano UTC` required, invalid format => `QueueHealthy=False`, manual enqueue only when queue is empty of non-`Done` snapshots).
- 2026-02-23: locked A-2 controller topology (`BackupPolicy` + `Backup`), `VolumeSnapshot` secondary watch in `BackupPolicy` controller, and failed-head blocking/unblock rules.
- 2026-02-23: watch predicate contract added and v1 status model clarified as conditions-first (no dedicated phase field).

## Read First

- core-beliefs.md
- operator-architecture.md
- restore-volume-populator-contract.md
- backup-target-node-writer-contract.md
- backup-data-model.md
- storage-format-v1.md
- crd-design.md
- crd-schema-draft.md
- controller-reconcile-contract.md

## Data and Lifecycle

- manifest-format-options.md
- gc-strategy.md

## Risk and Gaps

- requirements-gap-analysis.md

## Change Policy

- Add a new doc for major design shifts.
- Update this index in the same change.
- Keep unresolved items explicit with status labels.
