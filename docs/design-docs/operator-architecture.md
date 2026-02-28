# Operator Architecture (v1alpha1 Locked)

## Objective

Coordinate snapshot-based backup/restore for RBD PVCs with declarative control and serial transfer.

Detailed node placement / writer protocol contract:
- `/Users/molpako/src/github.com/molpako/rbd-local-backup/docs/design-docs/backup-target-node-writer-contract.md`
Detailed restore contract:
- `/Users/molpako/src/github.com/molpako/rbd-local-backup/docs/design-docs/restore-volume-populator-contract.md`

## API Surface

- BackupPolicy
  - schedule and retention parent
  - serial dispatch guard owner
- Backup
  - single transfer execution
  - input is `spec.volumeSnapshotRef`
  - destination is `spec.destination.nodeName` (set by `BackupPolicy` controller)
  - writer progress is exposed via `status.writer`
  - restore metadata is exposed via `status.restoreSource`
- VolumeSnapshot (existing)
  - consistency point and queue carrier
- PersistentVolumeClaim (existing)
  - restore request entrypoint via `spec.dataSourceRef -> Backup`
- VolumePopulator (existing CRD)
  - registration `sourceKind={group:snaplane.molpako.github.io, kind:Backup}`

## Queue Metadata on VolumeSnapshot

- label: `snaplane.molpako.github.io/backup-state=Pending|Dispatched|Done|Failed`
- annotation: `snaplane.molpako.github.io/queueTime=<RFC3339>`
- annotation: `snaplane.molpako.github.io/backupName=<backup>`
- label (manual association): `snaplane.molpako.github.io/policy=<name>`

## Controller Topology (A-2 + External Restore Populator)

- `BackupPolicy` controller
  - single writer for snapshot queue metadata
  - watches:
    - `BackupPolicy` (primary)
    - `VolumeSnapshot` (secondary; `status.readyToUse` transition trigger)
    - `Backup` (secondary; terminal transition trigger)
  - internal phases:
    - scheduler: create-or-get deterministic `VolumeSnapshot`
    - dispatcher: strict serial evaluation + CAS dispatch + `Backup` create-or-get
    - completion observer: set snapshot `Done`/`Failed`
    - retention: delete expired `Done` snapshots and linked `Backup` (skip when active restore references the `Backup`)
- `Backup` controller
  - watches `Backup` (primary)
  - executes transfer from `Backup.spec.volumeSnapshotRef`
  - updates `Backup.status` only
  - does not mutate snapshot queue metadata
- restore populator controller
  - watches PVC (primary, `spec.dataSourceRef`)
  - accepts only `Backup` datasource (`apiGroup=snaplane.molpako.github.io`, `kind=Backup`)
  - validates `Backup` completion and `status.restoreSource` before restore start
  - restores CAS data into lib-volume-populator `pvcPrime` and lets machinery rebind PV

## Watch Predicates (v1)

`BackupPolicy` controller event filters:
- `BackupPolicy` primary:
  - create events
  - update when `metadata.generation` changed
  - update when `reconcile.snaplane.molpako.github.io/requestedAt` changed
  - update when `deletionTimestamp` changed
- `VolumeSnapshot` secondary:
  - update when `status.readyToUse` transitions `false -> true`
  - update when queue metadata keys changed (`backup-state`, `queueTime`, `policy`, `backupName`)
  - delete events
- `Backup` secondary:
  - create when `spec.policyRef.name` is set (manual Route A)
  - update when backup transitions non-terminal -> terminal (`Completed` or `Failed`)

## Dispatch Rules

- strict order: `queueTime` ascending, tie-breaker `metadata.name` ascending
- serial guard: if one `Backup` is non-terminal (`Completed`/`Failed` 以外), do not create another
- no overtaking: if the oldest non-`Done` snapshot is not dispatchable, do not evaluate later snapshots
- failed-head blocking: if the oldest non-`Done` snapshot is `Failed`, stop dispatch until that same snapshot completes via manual retry

Dispatchable oldest snapshot:
- state is `Pending`
- `VolumeSnapshot.status.readyToUse=true`
- no active backup (terminal conditions `Completed`/`Failed` が未成立) in the same policy scope

## Backup Target Node Placement (v1)

`BackupPolicy` controller は `Backup` create 時に destination node を確定する。

candidate 条件:
- Node label `snaplane.molpako.github.io/backup-target=true`
- Node Ready condition `True`
- node Lease (`backup-node-<nodeName>`) が存在し `renewTime` が stale でない（<= 90s）
- Lease annotation `writer-endpoint`, `used-bytes`, `available-bytes` が有効
- `available-bytes >= 10GiB`

選定ルール:
- PVC annotation `assigned-backup-node` があり、かつ candidate 条件を満たす場合は同ノードを継続利用する。
- `assigned-backup-node` が candidate 条件を満たさない場合は destination を `unavailable` とする。
- 未割当の場合は `used-bytes` 最小を選ぶ。
- 同値は `nodeName` 昇順で tie-break。
- 初回選定後は PVC annotation に assignment を patch して固定する。

failure ルール:
- 割当ノードに到達不能な場合は `Backup` を `Failed` にする。
- v1 では自動フェイルオーバーしない（assignment を維持）。

## Writer Sidecar Plane (v1)

writer sidecar は `backup-target=true` ノード上の DaemonSet として稼働する。

責務:
- `/var/backup` 配下への node-local 書込み
- 30 秒ごとの Lease heartbeat 更新
- Lease annotation で endpoint と容量（used/available）を公開

通信契約:
- gRPC service: `StartWrite`, `WriteFrames` (client streaming), `CommitWrite`, `AbortWrite`
- frame type: `DATA`, `ZERO`, `EOF`
- controller は Lease annotation `writer-endpoint` を見て接続
- 通信は mTLS 必須（sidecar: `RequireAndVerifyClientCert`）

## Reconcile Stage Model

BackupPolicy controller stages:
- ScheduleTick
- SnapshotEnsured
- QueueMetadataReconciled
- DispatchGateEvaluated
- Dispatched or Waiting or Blocked

Backup execution states:
- Pending
- Running
- Completed or Failed

Each stage must be resumable from world state.

## Snapshot Metadata Patch Cases

After `create-or-get` of a scheduled snapshot:

1. NotFound -> create with initial metadata (`Pending`, `queueTime`, `policy`).
2. Found and `backup-state` is empty -> patch missing queue metadata.
3. Found and `backup-state=Pending` -> patch only missing metadata fields.
4. Found and `backup-state=Dispatched|Done|Failed` -> do not patch queue state.
5. Found and `policy` label exists with a different value -> do not patch (treat as ownership conflict).

## Manual Entry Points

### Route A: Existing VolumeSnapshot -> Backup

- user creates `Backup` with `spec.volumeSnapshotRef`
- `BackupPolicy` controller complements queue metadata only when needed
- `Dispatched|Done|Failed` is never auto-rewound to `Pending`
- manual retry of the failed head snapshot is the unblock path for queue progress

### Route B: Requeue BackupPolicy

- user updates annotation:
  `reconcile.snaplane.molpako.github.io/requestedAt=<value>`
- `value` must be `RFC3339Nano UTC` (example: `2026-02-23T12:34:56.123456789Z`)
- if changed from last seen value, controller evaluates immediate queue action and updates `status.lastRequestedAtSeen`
- invalid format does not enqueue manually; controller reports `QueueHealthy=False` with reason `InvalidRequestedAtFormat`
- valid trigger still does not overtake: if any non-`Done` snapshot exists, do not enqueue additional snapshot
- schedule processing remains active even when `requestedAt` is invalid
- temporary `spec.schedule.cron` rewrites for run-now are not a supported route in v1alpha1

### Route C: Restore by PVC DataSourceRef

- user creates PVC with:
  - `spec.dataSourceRef.apiGroup=snaplane.molpako.github.io`
  - `spec.dataSourceRef.kind=Backup`
  - `spec.dataSourceRef.name=<completed-backup-name>`
- populator validates:
  - `Backup.status.conditions` includes `Completed=True`
  - `Failed=True` is not set
  - `Backup.status.restoreSource.repositoryPath` and `manifestID` are present
- on validation success, populator starts CAS restore to `pvcPrime`
- after completion, library performs PV rebind to original PVC

## Requeue Strategy

- always return `RequeueAfter` for next cron boundary
- use `VolumeSnapshot` watch (`readyToUse` transition) as primary wait wake-up
- optional short safety requeue (1-2 minutes) to recover from missed watch events

## Implementation Principles

- deterministic names for snapshots and backups
- deterministic names for restore workers (`restore-<pvcUID>`)
- idempotent create-or-get behavior
- state decisions based on observed world (`VolumeSnapshot`/`Backup`)
- status is for reporting, not primary source of truth
- queue metadata ownership remains in `BackupPolicy` controller only
