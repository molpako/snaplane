# Controller Reconcile Contract (v1alpha1)

## Purpose

`BackupPolicy` controller と `Backup` controller の責務境界と、再起動/競合時の不変条件を明文化する。

## Shared Invariants

1. operator 所有 controller は 2 つのみ（`BackupPolicy` controller と `Backup` controller）。
2. restore は external `VolumePopulator` controller が担当し、operator 内に `Restore` controller は追加しない。
3. queue ownership は `BackupPolicy` controller が持つ（single writer）。
4. strict serial は同一 `BackupPolicy` スコープで保証する。
5. dispatcher は常に「最古 non-Done 1件」だけを判定する。
6. oldest non-Done が未Readyなら後続を評価しない（no overtaking）。
7. oldest non-Done が `Failed` なら dispatch を停止する（failed-head blocking）。
8. `Pending -> Dispatched` は CAS 成功時のみ遷移可能。
9. controller は `spec` を変更しない。
10. `status` は差分がある時のみ更新する。
11. `Backup.spec.destination.nodeName` は `BackupPolicy` controller が作成時に確定する。
12. backup node assignment は PVC annotation に保持し、v1 では自動フェイルオーバーしない。

## Queue Keys

- label `snaplane.molpako.github.io/backup-state`
- annotation `snaplane.molpako.github.io/queueTime`
- annotation `snaplane.molpako.github.io/backupName`
- label `snaplane.molpako.github.io/policy`
- annotation `reconcile.snaplane.molpako.github.io/requestedAt`

node placement keys:
- Node label `snaplane.molpako.github.io/backup-target=true`
- PVC annotation `snaplane.molpako.github.io/assigned-backup-node`
- PVC annotation `snaplane.molpako.github.io/assigned-backup-node-at`
- Lease annotation `snaplane.molpako.github.io/writer-endpoint`
- Lease annotation `snaplane.molpako.github.io/used-bytes`
- Lease annotation `snaplane.molpako.github.io/available-bytes`

## BackupPolicy Controller Contract

### Watches

1. primary: `BackupPolicy`
   - create
   - update on `metadata.generation` change
   - update on `reconcile.snaplane.molpako.github.io/requestedAt` change
   - update on `deletionTimestamp` change
2. secondary: `VolumeSnapshot`
   - update on `status.readyToUse: false -> true`
   - update on queue metadata key changes (`backup-state`,`queueTime`,`policy`,`backupName`)
   - delete
3. secondary: `Backup`
   - create when `spec.policyRef.name` is set
   - update on terminal conditions transition (`Completed` or `Failed`)

### Scheduler Loop

1. trigger: cron tick, `BackupPolicy` spec update, `requestedAt` change。
2. action: 決定的名 `VolumeSnapshot` を create-or-get。
3. action: snapshot metadata は以下の分岐で補完:
   - NotFound: 初期 metadata (`Pending`,`queueTime`,`policy`) を付けて create
   - Found + state empty: `Pending`,`queueTime`,`policy` を patch
   - Found + `Pending`: 欠落 metadata のみ patch
   - Found + `Dispatched|Done|Failed`: patch しない
   - Found + `policy` が別値: patch しない（ownership conflict）
4. action: `status.lastScheduledTime` を更新。

### Dispatcher Loop

1. `policy` label 対象 snapshot を list し、queue state (`Pending|Dispatched|Done|Failed`) を対象にする。
2. `queueTime ASC`, tie-breaker `metadata.name ASC` で sort。
3. 先頭から最初の non-`Done` snapshot を gate として選ぶ。
4. gate が `Failed` の場合は dispatch しない（blocked）。
5. gate が `Dispatched` の場合は dispatch しない（completion wait）。
6. gate が `Pending` かつ `ReadyToUse != true` の場合は dispatch しない（ready wait）。
7. 同一 policy で terminal conditions (`Completed|Failed`) を満たさない `Backup` があれば dispatch しない（single flight）。
8. gate が `Pending` かつ `ReadyToUse=true` の時だけ CAS patch で `Pending -> Dispatched`。
9. CAS 成功後、`Backup` 作成前に destination node を解決する:
   - candidate: `backup-target=true` + `Ready=True` + fresh Lease (<=90s) + `available>=10GiB`
   - PVC assignment 未設定時は `used-bytes` 最小（同値 nodeName 昇順）を選定して PVC annotation に patch
   - PVC assignment 設定済みで candidate 条件を満たす場合のみ同一ノードを継続利用
   - PVC assignment 設定済みで candidate 条件を満たさない場合は destination を `unavailable`
   - candidate 不足時は destination を `unavailable` として `Backup` を作成
10. destination 決定後、対応 `Backup` を create-or-get。
11. `status.lastDispatchedSnapshot` / `status.activeBackupName` を更新。

### Completion Observer Loop

1. policy 対象の `Backup` を監視。
2. `Completed` を観測したら対象 snapshot を `Done` + `backupName` に patch。
   - manual retry で `Failed` snapshot が完了した場合も `Done` へ遷移させる。
3. `Failed` を観測したら対象 snapshot を `Failed` + `backupName` に patch。
4. `status.activeBackupName` は実行中が無ければ空に戻す。

### Retention Loop

1. 対象: `backup-state=Done` かつ `policy=<name>`。
2. `queueTime` を基準に `keepDays` 超過分を古い順に選定。
3. snapshot と対応 `Backup` を削除。ただし active restore 参照中の `Backup` は削除保留。
4. active restore 参照は、`dataSourceRef(apiGroup=snaplane.molpako.github.io, kind=Backup, name=<backup>)` を持つ PVC のうち
   `spec.volumeName==""` または populator finalizer あり、で判定する。
5. `backup-state=Failed` は削除対象に含めない。

## Backup Controller Contract

### Input Resolution

1. `spec.policyRef.name` と `spec.volumeSnapshotRef` を解決。
2. `volumeSnapshotRef.namespace` 未指定時は `Backup` の namespace を使う。
3. destination node から Lease 名 `backup-node-<nodeName>` を解決し、`writer-endpoint` を取得する。
4. 対象 snapshot 不在時は `Failed` へ遷移し `lastError` を設定。

### Execution

1. `Pending -> Running` に status 更新。
2. writer gRPC `StartWrite` を実行し、`status.writer.sessionID/lastAckedOffset/targetPath` を反映。
3. `WriteFrames`（client streaming）で `DATA/ZERO/EOF` を送信し、summary を受信する。
4. `CommitWrite` 成功時に `stats.bytesTransferred` と `status.writer` を確定する。
5. `Completed=True` を立てる前に `status.restoreSource`（`nodeName/repositoryPath/manifestID/repoUUID`）を確定する。
6. 失敗時は reason を分類して `Failed` + `completionTime` + `lastError` を設定する。
7. queue metadata (`backup-state`) は更新しない。

## Restore Populator Contract

1. restore 要求は PVC `spec.dataSourceRef` で受け取り、`Backup` CR を参照する。
2. populator は `Backup` が `Completed=True` かつ `Failed!=True` の場合のみ受理する。
3. `Backup.status.restoreSource.repositoryPath` と `manifestID` が欠落している場合は restore を開始しない。
4. populator 実行は lib-volume-populator provider mode (`PopulateFn`, `PopulateCompleteFn`, `PopulateCleanupFn`) を使う。
5. operator は restore の専用 CRD/controller を持たない。

## Manual Routes

### Route A: User-created Backup

1. `Backup` 作成を検知した `BackupPolicy` controller が対象 snapshot の queue metadata を補完。
2. `Dispatched|Done|Failed` snapshot は `Pending` へ巻き戻さない。
3. oldest `Failed` snapshot への manual retry は唯一の unblock 手段として扱う。
4. 通常 dispatcher フローに合流させる。

### Route B: requestedAt Requeue

1. `requestedAt` 値は `RFC3339Nano UTC`（`...Z`）を要求する。
2. 値が `status.lastRequestedAtSeen` と異なる時だけ manual trigger を評価し、処理後 `lastRequestedAtSeen` を更新する。
3. 形式不正の場合、manual enqueue は行わず `QueueHealthy=False` (`reason=InvalidRequestedAtFormat`) を設定する。
4. 既存 non-`Done`（`Pending|Dispatched|Failed`）snapshot がある間は manual enqueue しない。
5. non-`Done` が 0 件の時だけ `queueTime=now` で 1 件 enqueue する。

### Route C: Restore via PVC DataSourceRef

1. ユーザーが PVC を作成し、`dataSourceRef` に `Backup` を指定する。
2. populator は `Completed=True` かつ `restoreSource` 完備を検証する。
3. 条件を満たせば CAS restore を実行し、`pvcPrime` 経由で最終 PVC へ rebind する。
4. 条件を満たさない場合は PVC Event Warning を記録して再試行待ちにする。

## Conflict and Retry Rules

1. metadata 更新は `Patch` を使用する。
2. CAS 失敗 (`409 Conflict`) は最新オブジェクト再取得後に全判定をやり直す。
3. status 更新は `Status().Patch` または `Status().Update` を差分時のみ実行する。
4. reconcile は常に冪等に保つ（create-or-get、observe-first）。
5. create 後の watch 反映遅延を前提に、同一 slot / 同一 snapshot の二重 create を create-or-get で吸収する。

## Requeue Rules

1. cron 次回時刻までの `RequeueAfter` を返す。
2. `VolumeSnapshot.status.readyToUse` の変化で即時再評価する。
3. 安全策として短周期（1-2分）の `RequeueAfter` を任意で併用してもよい。

## Conditions

- `BackupPolicy` conditions: `Ready`, `QueueHealthy`, `SerialGuardSatisfied`
- `Backup` conditions: `Accepted`, `Running`, `Completed`, `Failed`

ルール:
- condition transition は execution state 変化時にだけ更新する。
- oldest non-Done が `Failed` の間は `SerialGuardSatisfied=False` とし、
  reason は `BlockedByFailedSnapshot` を使う。
- `requestedAt` 形式不正時は `QueueHealthy=False` とし、
  reason は `InvalidRequestedAtFormat` を使う。

## Failure Handling

1. `Failed` snapshot は自動再投入しない。
2. oldest non-Done が `Failed` の場合、後続 `Pending` の dispatch は停止する。
3. 解除は failed snapshot への manual retry (`Backup` 作成) が `Completed` になる場合のみ。
4. `requestedAt` 更新は即時評価トリガーであり、failed-head blocking 自体は解除しない。
5. 同一失敗を連続で上書きせず、最終エラーを `Backup.status.lastError` に保持する。
6. `spec.schedule.cron` の一時変更を即時発火トリガーとして扱うことは v1alpha1 契約外（非推奨）。
7. destination node が `unavailable` または endpoint 解決不能の場合、`Backup` は `AssignedNodeUnavailable` で失敗する。
8. writer 接続不能は `WriterEndpointNotReady`、stream/commit 失敗は `WriteStreamFailed`/`WriteCommitFailed` を使う。
