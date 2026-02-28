# Restore VolumePopulator Contract (v1alpha1 Locked)

## Objective

`Restore` 専用 CRD / controller を追加せず、`Backup` CR を `PersistentVolumeClaim.spec.dataSourceRef` で直接参照して復元を成立させる。

本ドキュメントは以下を固定する。
- `VolumePopulator` 登録 (`sourceKind: Backup`)
- `Completed` な `Backup` のみ受理する受理条件
- CAS repository から target PVC へ復元する実行モデル
- retention と restore 実行の競合時の保護ルール

## Scope

このドキュメントの対象:
- restore の API エントリポイント（PVC + `dataSourceRef`）
- populator controller の受理/拒否条件
- `Backup.status` に必要な restore 参照情報
- cross-namespace の取り扱い
- 最小 RBAC と failure reason

このドキュメントの対象外:
- restore engine の最適化（prefetch, parallel read, sparse hole 最適化）
- writer sidecar の read API 新設
- 既存 backup 実行フロー（別ドキュメントで固定済み）

## Locked Decisions

1. v1alpha1 では `Restore` CRD / `Restore` controller を導入しない。
2. restore 要求は PVC 作成時の `spec.dataSourceRef` のみを正規入口とする。
3. `dataSource` ではなく `dataSourceRef` を必須とし、`apiGroup/kind/name` に `Backup` を指定する。
4. `VolumePopulator` は `sourceKind: {group: snaplane.molpako.github.io, kind: Backup}` を登録する。
5. populator は `Backup.status.conditions` の `Completed=True` を満たす `Backup` のみ受理する。
6. restore 実行は CAS repository を読む restore worker で行い、結果を `pvcPrime` に書き込む。
7. cross-namespace restore はデフォルト無効（same namespace のみ）。
8. retention 削除は「進行中 restore が参照中の Backup」を削除しない。

## Restore Request Contract

restore request は以下の PVC で表現する。

```yaml
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: restore-pvc
  namespace: app-a
spec:
  storageClassName: csi-rbd
  accessModes: ["ReadWriteOnce"]
  resources:
    requests:
      storage: 10Gi
  dataSourceRef:
    apiGroup: snaplane.molpako.github.io
    kind: Backup
    name: backup-2026-03-01-010000
```

ルール:
- `dataSourceRef` の `apiGroup/kind/name` は required。
- v1alpha1 default では `dataSourceRef.namespace` は使わない（same namespace 限定）。
- PVC 作成後に `dataSourceRef` を変更する運用は不可（PVC spec immutable 前提）。

## VolumePopulator Registration

```yaml
apiVersion: populator.storage.k8s.io/v1beta1
kind: VolumePopulator
metadata:
  name: snaplane-backup-populator
sourceKind:
  group: snaplane.molpako.github.io
  kind: Backup
```

## Backup Acceptance Contract

populator が restore を開始してよい条件:
1. `Backup` が存在する。
2. `Backup` の terminal 判定が `Completed=True` である。
3. `Backup` で `Failed=True` が立っていない。
4. `Backup.status.restoreSource` の required 項目が埋まっている。

上記を満たさない場合は拒否し、PVC Event を Warning で残す。

## Backup Status Extension

`Backup.status` に restore 用参照情報を追加する。

```go
type BackupRestoreSourceStatus struct {
    // Backup data is stored on this node-local repository host.
    NodeName string `json:"nodeName,omitempty"`

    // Absolute repository root path (example: /var/backup/ns/pvc/backup-name/repo).
    RepositoryPath string `json:"repositoryPath,omitempty"`

    // Target generation identifier in snapshots/<manifestID>/manifest.json.
    ManifestID string `json:"manifestID,omitempty"`

    // Integrity/compatibility anchor copied from repo.json.
    RepoUUID string `json:"repoUUID,omitempty"`

    // Optional read contract hints.
    VolumeSizeBytes int64 `json:"volumeSizeBytes,omitempty"`
    ChunkSizeBytes  int64 `json:"chunkSizeBytes,omitempty"`
}
```

`Backup.status` への追加:

```go
RestoreSource BackupRestoreSourceStatus `json:"restoreSource,omitempty"`
```

書込みタイミング:
- `Backup` が `Completed=True` へ遷移する直前までに `restoreSource` を確定する。
- restore に必要な path/manifest が未確定なら `Completed=True` にしない。

## Populator Execution Model (Provider Mode)

v1alpha1 restore populator は `ProviderFunctionConfig` を使う。
- `PopulateFn`: restore 開始（restore worker create-or-get）
- `PopulateCompleteFn`: restore 完了判定
- `PopulateCleanupFn`: 一時 resource cleanup

理由:
- `Completed` 判定と `restoreSource` 検証が必要。
- node-local source (CAS repository) と `pvcPrime` を同時に扱う restore worker を制御する必要がある。
- retry / idempotency を Job 名固定で制御しやすい。

restore worker 契約:
- input: `repositoryPath`, `manifestID`, `pvcPrime`。
- output: `pvcPrime` 上に復元済み block/image。
- worker 名は `restore-<pvcUID>` の決定的命名。
- `PopulateFn` は create-or-get（重複作成禁止）。

## CAS Restore Procedure

1. `manifestID` から親チェーンを解決。
2. chunk index -> chunk ID の最終マップを構築。
3. active index から chunk location を解決。
4. pack を offset 順に読み出す。
5. target 側 chunk offset に書き込む。
6. unallocated chunk は zero write（または sparse hole）。

上記の読出し元フォーマットは `storage-format-v1.md` に従う。

## Retention Guard For Restore

retention 対象選定は従来どおり `backup-state=Done` + `keepDays` 超過を使うが、
以下を満たす Backup は削除保留とする。

- 同 namespace に `dataSourceRef(apiGroup=snaplane.molpako.github.io, kind=Backup, name=<backup>)` を持つ PVC が存在し、かつ
  `spec.volumeName==""`（未bind）または populator finalizer が残っている。

これにより「復元中に retention が source Backup を削除する」競合を回避する。

注意:
- bind 完了後の PVC は immutable な `dataSourceRef` を保持し続けるため、
  `spec.volumeName!=""` かつ finalizer 無しの PVC は保護条件に含めない。

## Cross-Namespace Contract

default:
- `dataSourceRef.namespace` は使わない（same namespace のみ）。

将来有効化時の要件:
1. Kubernetes 側 feature gate `CrossNamespaceVolumeDataSource=true`。
2. populator 側 `CrossNamespace=true`。
3. referent namespace に `ReferenceGrant` を配置。
4. populator SA に `gateway.networking.k8s.io/referencegrants` の read 権限。

## RBAC Contract (Populator)

必須権限:
- core: `persistentvolumeclaims`, `persistentvolumes`, `pods`, `events`
- storage.k8s.io: `storageclasses`
- snaplane.molpako.github.io: `backups`

cross-namespace 有効時のみ追加:
- gateway.networking.k8s.io: `referencegrants`

## Failure Reasons (PVC Events)

- `PopulatorDataSourceNotFound`: 参照 `Backup` 不在
- `BackupNotCompleted`: `Completed=True` 未達
- `BackupFailed`: `Failed=True`
- `RestoreSourceInvalid`: `status.restoreSource` 欠落/不整合
- `RestoreJobCreateFailed`: restore worker 作成失敗
- `RestoreExecutionFailed`: CAS read or target write 失敗

## Operational Constraints

- populator namespace とアプリ PVC namespace は分離する。
  - lib-volume-populator は controller 自身の namespace 上の PVC を処理対象外にするため。
- restore は CSI dynamic provisioning 前提。
- `AnyVolumeDataSource` を前提に運用する（1.33 以降 GA）。

## Evidence Anchors

Kubernetes core:
- `dataSourceRef` が non-core object を許容:
  - `/Users/molpako/src/github.com/kubernetes/kubernetes/staging/src/k8s.io/api/core/v1/types.go:588`
- custom kind を `dataSource` だけで渡した場合は旧互換で drop されうる:
  - `/Users/molpako/src/github.com/kubernetes/kubernetes/pkg/api/persistentvolumeclaim/util.go:64`
- `AnyVolumeDataSource` は 1.33 で GA:
  - `/Users/molpako/src/github.com/kubernetes/kubernetes/pkg/features/kube_features.go:1066`
- `CrossNamespaceVolumeDataSource` は Alpha default false:
  - `/Users/molpako/src/github.com/kubernetes/kubernetes/pkg/features/kube_features.go:1143`
- PVC spec immutable:
  - `/Users/molpako/src/github.com/kubernetes/kubernetes/pkg/apis/core/validation/validation.go:2509`

VolumePopulator / lib-volume-populator:
- `VolumePopulator.sourceKind` CRD:
  - `/Users/molpako/src/github.com/kubernetes/kubernetes/test/e2e/testing-manifests/storage-csi/any-volume-datasource/crd/populator.storage.k8s.io_volumepopulators.yaml:34`
- e2e で `dataSourceRef` に custom CR を指定:
  - `/Users/molpako/src/github.com/kubernetes/kubernetes/test/e2e/storage/testsuites/provisioning.go:451`
- ProviderFunctionConfig (`PopulateFn`, `PopulateCompleteFn`, `PopulateCleanupFn`):
  - `/Users/molpako/src/github.com/kubernetes-csi/lib-volume-populator/populator-machinery/controller.go:186`
- `syncPvc` の処理フロー（datasource 判定、pvcPrime、provider callbacks、cleanup）:
  - `/Users/molpako/src/github.com/kubernetes-csi/lib-volume-populator/populator-machinery/controller.go:615`
- controller namespace 上 PVC は処理対象外:
  - `/Users/molpako/src/github.com/kubernetes-csi/lib-volume-populator/populator-machinery/controller.go:616`
- cross namespace の `ReferenceGrant` 判定:
  - `/Users/molpako/src/github.com/kubernetes-csi/lib-volume-populator/populator-machinery/util.go:16`

Repository format:
- restore 時の generation/chunk 解決と pack read:
  - `/Users/molpako/src/github.com/molpako/rbd-local-backup/docs/design-docs/storage-format-v1.md`
