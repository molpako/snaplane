# CRD Design (v1alpha1 Locked)

## Objective

Ceph CSI `VolumeSnapshot` を整合点として使い、バックアップ取得と転送を宣言的に運用できる API を定義する。

必須要件:
- T時点で必ず `VolumeSnapshot` を作成する
- バックアップ転送は strict serial（同時実行なし）
- queue順序は `queueTime` 昇順で固定し、追い越しを禁止する

## API Surface

- group/version: `snaplane.molpako.github.io/v1alpha1`
- namespaced kinds:
  - `BackupPolicy`
  - `Backup`
- observed existing kind:
  - `snapshot.storage.k8s.io/v1 VolumeSnapshot`
  - `v1 PersistentVolumeClaim` (restore request entrypoint via `dataSourceRef`)
  - `populator.storage.k8s.io/v1beta1 VolumePopulator` (registration for `sourceKind=Backup`)

v1 では `BackupContent` は導入しない。

## Locked Decisions

1. キューモデルは `VolumeSnapshot` metadata（label/annotation）を採用する。
2. `backup-state` 遷移は `BackupPolicy` controller が一元管理する。
3. `Failed` の再投入は手動 `Backup` 作成（Route A）でのみ許可する。
4. retention は `retention.keepDays` に連動させるが、active restore が参照中の `Backup` は削除保留する。
5. dispatch mode は `SerialOnly` 固定（v1）。
6. 最古 non-`Done` が dispatch 不可なら後続 Ready を追い越さない。
7. controller topology は A-2 固定:
   - `BackupPolicy` controller
   - `Backup` controller
8. `BackupPolicy` controller は `VolumeSnapshot` を secondary watch する。
9. oldest non-`Done` snapshot が `Failed` の場合、後続 dispatch は停止する。
10. failed-head blocking の解除は failed snapshot の manual retry 成功のみ。
11. `Backup.spec.destination.nodeName` は `BackupPolicy` controller が必ず設定する。
12. backup destination node assignment は PVC annotation で固定化する。
13. node runtime status は Lease annotation で公開し、新規 CRD は追加しない。
14. writer sidecar 通信は mTLS 必須とする。
15. restore は `Restore` 専用 CRD を作らず、PVC `dataSourceRef -> Backup` で受け付ける。
16. restore populator は `Completed` な `Backup` のみ受理する。
17. restore populator は external component として扱い、A-2（`BackupPolicy`/`Backup`）には含めない。

## Queue Metadata Contract

`VolumeSnapshot` には以下を付与する。

- label: `snaplane.molpako.github.io/backup-state=Pending|Dispatched|Done|Failed`
- annotation: `snaplane.molpako.github.io/queueTime=<RFC3339>`
- annotation: `snaplane.molpako.github.io/backupName=<backup-name>`
- label: `snaplane.molpako.github.io/policy=<policy-name>`
- annotation: `reconcile.snaplane.molpako.github.io/requestedAt=<opaque-string>` (`BackupPolicy` 側)

PVC assignment metadata:
- annotation: `snaplane.molpako.github.io/assigned-backup-node=<node-name>`
- annotation: `snaplane.molpako.github.io/assigned-backup-node-at=<RFC3339>`

Node runtime metadata (Lease annotation):
- `snaplane.molpako.github.io/writer-endpoint=<podIP:9443>`
- `snaplane.molpako.github.io/used-bytes=<int64>`
- `snaplane.molpako.github.io/available-bytes=<int64>`

順序キー:
- 1st key: `queueTime` (ascending)
- 2nd key: `metadata.name` (ascending)

## Resource Responsibilities

### BackupPolicy

責務:
- cron schedule 管理
- `VolumeSnapshot` enqueue
- strict serial dispatcher
- retention cleanup
- queue metadata single-writer

`spec`:
- `source.persistentVolumeClaimName` (required, immutable, min length 1)
- `source.volumeSnapshotClassName` (required, immutable, min length 1)
- `schedule.cron` (required, non-empty)
- `schedule.timeZone` (optional)
- `schedule.suspend` (optional, default `false`)
- `dispatch.mode` (optional, enum `SerialOnly`, default `SerialOnly`)
- `retention.keepDays` (required, min `1`)
- `transfer.timeoutSeconds` (optional, min `1`)
- `transfer.backoffLimit` (optional, range `0..60`, default `6`)

`status`:
- `conditions[]`
- `lastScheduledTime`
- `lastDispatchedSnapshot`
- `activeBackupName`
- `pendingSnapshotCount`
- `lastRequestedAtSeen`

condition types:
- `Ready`
- `QueueHealthy`
- `SerialGuardSatisfied`

### Backup

責務:
- 1回の転送実行単位
- 入力 `VolumeSnapshot` を明示参照
- 転送進捗/結果を status 公開

`spec`:
- `policyRef.name` (required, immutable, min length 1)
- `volumeSnapshotRef.name` (required, immutable, min length 1)
- `volumeSnapshotRef.namespace` (optional, immutable if set; default self namespace)
- `destination.nodeName` (required, immutable)
- `transfer.timeoutSeconds` (optional override, min `1`)
- `transfer.backoffLimit` (optional override, range `0..60`)

`status`:
- `conditions[]`
- `startTime`
- `completionTime`
- `lastError`
- `stats.bytesTransferred`
- `writer.sessionID`
- `writer.lastAckedOffset`
- `writer.targetPath`
- `restoreSource.nodeName`
- `restoreSource.repositoryPath`
- `restoreSource.manifestID`
- `restoreSource.repoUUID`
- `restoreSource.volumeSizeBytes`
- `restoreSource.chunkSizeBytes`

condition types:
- `Accepted`
- `Running`
- `Completed`
- `Failed`

### VolumeSnapshot (existing)

責務:
- point-in-time consistency の提供
- `ReadyToUse` の観測対象
- queue metadata のキャリア
- `BackupPolicy` controller の secondary watch 対象

### PersistentVolumeClaim + VolumePopulator (restore entry)

責務:
- restore 要求は PVC `spec.dataSourceRef` で表現する。
- `dataSourceRef` には `apiGroup=snaplane.molpako.github.io, kind=Backup, name=<backup-name>` を指定する。
- `VolumePopulator` は `sourceKind={group:snaplane.molpako.github.io, kind:Backup}` で登録する。
- populator は `Backup.status.conditions` の `Completed=True` かつ `Failed!=True` のみ受理する。

## State Machine

queue state:
- `Pending -> Dispatched -> Done|Failed`
- manual retry success path: `Failed -> Done`

ルール:
1. `Pending -> Dispatched` は CAS (`resourceVersion`) 成功時のみ許可。
2. dispatcher は policy 対象 queue を `queueTime ASC`, tie-breaker `metadata.name ASC` で評価する。
3. dispatcher は最古 non-`Done` 1件だけを判定する。
4. gate snapshot が `Pending` かつ `ReadyToUse!=true` なら待機して後続を見ない。
5. gate snapshot が `Dispatched` なら completion を待機し、後続を見ない。
6. gate snapshot が `Failed` なら blocked として後続 dispatch を停止する。
7. 同一 policy 内に terminal 条件（`Completed`/`Failed`）でない `Backup` があれば dispatch しない。
8. `Backup` の terminal state を観測して `VolumeSnapshot` を `Done`/`Failed` に更新する。

## Reconcile Flows

### Normal Schedule Route

1. `BackupPolicy` controller が cron tick で決定的名 `VolumeSnapshot` を create-or-get。
2. snapshot metadata は分岐適用:
   - NotFound: 初期 metadata 付きで create
   - Found + state empty: `Pending` + `queueTime` + `policy` を patch
   - Found + `Pending`: 欠落 metadata のみ patch
   - Found + `Dispatched|Done|Failed`: patch しない
   - Found + `policy` 別値: patch しない（ownership conflict）
3. dispatcher が strict order を満たす最古1件を CAS dispatch。
4. CAS成功後のみ決定的名 `Backup` を create-or-get。
5. `Backup` controller が転送実行し `status.conditions` を更新。
6. `BackupPolicy` controller が結果観測後 `Done`/`Failed` と `backupName` を patch。

### Manual Route A (User creates Backup)

1. ユーザーが `Backup.spec.volumeSnapshotRef` を指定して `Backup` を作成。
2. controller が対象 snapshot に queue metadata を補完:
   - `policy` label
   - `queueTime` (未設定なら現在時刻)
   - `backup-state=Pending` (`Dispatched|Done|Failed` は上書きしない)
3. oldest failed snapshot への manual retry が成功した場合のみ blocked queue を解除する。
4. 以後は通常 dispatcher に合流。

### Manual Route B (requestedAt requeue)

1. ユーザーが `BackupPolicy` annotation `reconcile.snaplane.molpako.github.io/requestedAt` を `RFC3339Nano UTC` 形式（例: `2026-02-23T12:34:56.123456789Z`）で変更。
2. controller は `status.lastRequestedAtSeen` と差分がある場合のみ manual trigger を評価し、処理済みマーカーとして `lastRequestedAtSeen` を更新。
3. `requestedAt` が形式不正の場合、manual enqueue は行わず `BackupPolicy` condition `QueueHealthy=False`（reason: `InvalidRequestedAtFormat`）を設定する。
4. `requestedAt` が有効でも、同一 policy に non-`Done` snapshot（`Pending|Dispatched|Failed`）が存在する間は新規 enqueue しない。
5. queue が empty の場合のみ `queueTime=now` で 1 件 enqueue する。
6. failed-head blocking が成立している場合、requestedAt では解除しない。

補足:
- `spec.schedule.cron` を一時変更して即時発火させる運用は v1alpha1 の正規ルートとしては扱わない（非推奨）。

## Requeue Strategy

1. controller は次回 cron 境界までの `RequeueAfter` を返す。
2. `VolumeSnapshot.status.readyToUse` 変化は secondary watch で即時再評価する。
3. watch 取りこぼし保険として短周期 `RequeueAfter`（1-2分）を任意で併用する。

## Retention and Cleanup

1. 対象は `backup-state=Done` の snapshot。
2. `queueTime` を基準に `retention.keepDays` を超過したものから古い順に削除。
3. 削除対象の snapshot に対応する `Backup` は、active restore 参照が無い場合のみ削除する。
4. active restore 参照は以下で判定する:
   - `dataSourceRef(apiGroup=snaplane.molpako.github.io, kind=Backup, name=<backup>)` を持つ PVC が存在し、
   - その PVC が `spec.volumeName==""`（未bind）または populator finalizer を保持している。
5. `Failed` は自動削除しない（運用者判断で明示削除）。

## Update/Patch Strategy

1. snapshot metadata は `Patch` を基本とする。
2. CAS境界 (`Pending -> Dispatched`) は `resourceVersion` 前提で conflict retry。
3. `status` は `Status().Patch` または `Status().Update` を差分時のみ実行。
4. controller は `spec` を変更しない。

## Validation / Admission

1. immutable:
   - `BackupPolicy.spec.source.*`
   - `Backup.spec.policyRef`
   - `Backup.spec.volumeSnapshotRef.*`
2. required/default/min/max を CRD schema に明示。
3. `dispatch.mode` は `SerialOnly` のみ許可（v1）。
4. `Backup.spec.volumeSnapshotRef.name` は必須。

詳細 schema は以下を参照:
- `/Users/molpako/src/github.com/molpako/rbd-local-backup/docs/design-docs/crd-schema-draft.md`

## Assumptions / Defaults

1. `BackupPolicy` / `Backup` は namespaced。
2. `volumeSnapshotRef.namespace` 未指定時は `Backup` と同一 namespace。
3. strict serial スコープは同一 `BackupPolicy` 内。
4. reconcile の主入力は `spec + cluster state`、`status` は公開出力。
5. 将来 `BackupContent` を導入する場合は `v1.1+` で追加検討する。
6. external cron service は使わず、controller 内部の schedule 計算で運用する。
7. 即時発火は `requestedAt` annotation を唯一の正規手段とする。
8. restore は same namespace の `dataSourceRef` のみを default とし、cross namespace は将来拡張とする。

関連設計:
- `/Users/molpako/src/github.com/molpako/rbd-local-backup/docs/design-docs/backup-target-node-writer-contract.md`
- `/Users/molpako/src/github.com/molpako/rbd-local-backup/docs/design-docs/restore-volume-populator-contract.md`
