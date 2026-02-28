# Backup Target Node And Writer Contract (v1alpha1 Locked)

## Objective

バックアップ転送先を node-local filesystem に固定し、PVC ごとの宛先ノード選定と writer sidecar の通信契約を v1alpha1 で確定する。

## Scope

このドキュメントが確定する対象:
- backup target ノード選定ルール
- PVC への宛先ノード固定化ルール
- Node sidecar heartbeat と容量指標の公開方法
- Backup controller と writer sidecar 間 gRPC 契約
- writer sidecar のモック書込み契約
- mTLS と cert-manager 前提

このドキュメントの対象外:
- 実データの差分取得/フレーム生成アルゴリズム
- 自動フェイルオーバー
- 容量しきい値や heartbeat 間隔の動的チューニング

## Locked Decisions

1. backup destination 候補は label `snaplane.molpako.github.io/backup-target=true` ノードのみ。
2. 割当単位は PVC。初回だけ選定し、以後は PVC annotation で固定する。
3. 初回選定は `usedBytes` 最小ノードを選ぶ。同値は `nodeName` 昇順。
4. sidecar 状態は `coordination.k8s.io/v1 Lease` で報告し、新規 CRD は導入しない。
5. 割当ノード不達・書込不能時は `Backup` を `Failed` にし、PVC 割当は維持する。
6. writer 通信は mTLS 必須。server/client 証明書は cert-manager 管理。
7. writer gRPC は `StartWrite` + client streaming `WriteFrames` + `CommitWrite` + `AbortWrite`。
8. v1alpha1 では writer 実装はモック書込みを最小成立要件とする。

## Metadata Contract

### Node label

- key: `snaplane.molpako.github.io/backup-target`
- value: `"true"`

### PVC annotations

対象 PVC: `BackupPolicy.spec.source.persistentVolumeClaimName`

- `snaplane.molpako.github.io/assigned-backup-node=<nodeName>`
- `snaplane.molpako.github.io/assigned-backup-node-at=<RFC3339>`

### Lease contract (operator namespace)

- name: `backup-node-<nodeName>`
- `spec.renewTime`: heartbeat 時刻
- annotations:
  - `snaplane.molpako.github.io/writer-endpoint=<podIP:9443>`
  - `snaplane.molpako.github.io/used-bytes=<int64>`
  - `snaplane.molpako.github.io/available-bytes=<int64>`

## Scheduler Contract

### Candidate filter

候補ノードは以下すべてを満たすこと:
1. `backup-target=true` label を持つ。
2. Node condition `Ready=True`。
3. Lease が存在し、`now - renewTime <= 90s`。
4. Lease annotation に endpoint/used/available が揃っている。
5. `availableBytes >= 10GiB`。

### Selection and pinning

1. PVC に `assigned-backup-node` が未設定:
   - 候補から `usedBytes` 最小を選ぶ。
   - 同値は `nodeName` 昇順。
   - PVC annotation を patch して固定化する。
   - patch conflict は再取得して再判定する（楽観ロック）。
2. PVC に `assigned-backup-node` が設定済み:
   - そのノードが candidate 条件を満たす間は継続利用する（再選定しない）。
   - candidate 条件を満たさない場合は destination を `unavailable` とする。
3. 候補ゼロまたは入力不備:
   - `Backup.spec.destination.nodeName="unavailable"` を設定し、
     実行段階で `AssignedNodeUnavailable` として失敗させる。

## Backup API Contract

`Backup.spec`:
- `destination.nodeName` を required/immutable で持つ。

`Backup.status`:
- `writer.sessionID`
- `writer.lastAckedOffset`
- `writer.targetPath`

## Writer gRPC Contract

service:
- `StartWrite(StartWriteRequest) returns (StartWriteResponse)`
- `WriteFrames(stream WriteFrame) returns (WriteFramesSummary)`
- `CommitWrite(CommitWriteRequest) returns (CommitWriteResponse)`
- `AbortWrite(AbortWriteRequest) returns (AbortWriteResponse)`

frame body:
- `DATA` (`offset`, `payload`, `crc32c`)
- `ZERO` (`offset`, `length`)
- `EOF` (`logical_size`)

session rules:
1. `StartWrite` は `backup_uid` 単位で冪等。
2. `WriteFrames` は contiguous offset を要求する。
3. `accepted_offset` より後退した frame は拒否する。
4. `CommitWrite` は `EOF` 受信済み session のみ許可する。
5. stream 異常時は `AbortWrite` で中断できる。

## Mock Writer Behavior (v1)

パス:
- `/var/backup/<namespace>/<pvc_name>/<backup_name>/`

ファイル:
- `mock.img.tmp` に `DATA` を `pwrite`。
- `ZERO` は zero-fill 書込み（hole punch は任意）。
- `CommitWrite` で `mock.img` へ rename。
- `meta.json` に session と統計を保存。

## Security Contract (mTLS)

1. sidecar server は `RequireAndVerifyClientCert` を使用する。
2. backup controller は client cert を提示し、CA 検証を行う。
3. endpoint discovery は Lease annotation `writer-endpoint` を使う。
4. cert-manager で server/client 証明書を発行/更新する。

## DaemonSet / RBAC Contract

1. DaemonSet は `nodeSelector: backup-target=true` のみに配置。
2. `hostPath /var/backup` を read-write mount する。
3. heartbeat は 30 秒間隔で Lease 更新。
4. sidecar SA には `leases` の `get/list/watch/create/update/patch` を付与。
5. controller SA には `nodes` `leases` `persistentvolumeclaims` の読み/patch 権限を付与。

## Failure Reasons

- `AssignedNodeUnavailable`
- `WriterEndpointNotReady`
- `WriteSessionStartFailed`
- `WriteStreamFailed`
- `WriteCommitFailed`

## Defaults

- heartbeat interval: `30s`
- stale threshold: `90s`
- minimum available bytes: `10GiB`
- automatic failover: disabled (v1)

## Evidence Anchors

- external-snapshot-metadata stream resume style:
  - `/Users/molpako/src/github.com/kubernetes-csi/external-snapshot-metadata/proto/schema.proto:64`
- external-snapshot-metadata stream timeout constant:
  - `/Users/molpako/src/github.com/kubernetes-csi/external-snapshot-metadata/pkg/sidecar/sidecar.go:43`
- ceph-csi CSI stream surface (metadata oriented):
  - `/Users/molpako/src/github.com/ceph/ceph-csi/vendor/github.com/container-storage-interface/spec/lib/go/csi/csi_grpc.pb.go:1004`
- Ceph RBD diff record semantics (`w`/`z`):
  - `/Users/molpako/src/github.com/ceph/ceph/doc/dev/rbd-diff.rst:47`
