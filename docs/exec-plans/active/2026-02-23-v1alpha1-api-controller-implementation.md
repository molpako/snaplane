# Active Exec Plan: v1alpha1 API and Controller Implementation

Date: 2026-02-23
Status: active

## Objective

Implement `snaplane.molpako.github.io/v1alpha1` API types and controller behavior exactly as locked in design docs.

## Scope

- Add API types for `BackupPolicy` and `Backup`
- Generate CRD manifests
- Implement exactly two controllers (`BackupPolicy`, `Backup`)
- Implement queue selection, CAS dispatch patch, and status/condition helpers
- Add envtest integration tests for serial queue invariants
- Keep e2e out of default test path (build tag gated)

## Progress Update (2026-02-28)

Implemented:
- `Backup.spec.destination.nodeName` + `Backup.status.writer` API surface and controller wiring.
- backup-target node scheduler with Lease-based candidate filtering (`Ready`, fresh heartbeat, min available bytes).
- PVC assignment annotation persistence and retry-on-conflict behavior.
- writer sidecar gRPC mock path (`StartWrite` / streaming `WriteFrames` / `CommitWrite` / `AbortWrite`).
- mTLS wiring in writer sidecar / controller client and cert-manager manifests.
- e2e lane split:
  - fast lane (`E2E_TLS_MODE=static`) without cert-manager install, static mTLS secrets generated in setup.
  - nightly lane (`E2E_TLS_MODE=cert-manager`) with cert-manager webhook readiness probe.
- failure reason mapping (`AssignedNodeUnavailable`, `WriterEndpointNotReady`, `WriteSessionStartFailed`, `WriteStreamFailed`, `WriteCommitFailed`).
- P0/P1 test additions:
  - scheduler tie-break + stale exclusion + assigned-node availability contract
  - PVC patch conflict retry unit test
  - writer resume (`accepted_offset`) + offset gap + CRC mismatch + path isolation
  - backup-controller failure reason coverage
  - mTLS tests (no client cert reject / valid cert success / wrong CA reject)

Remaining:
- publish deployment runbook for real cluster rollout and cert rotation operations.
- optional e2e pipeline gate for mTLS + writer heartbeat on a real Kubernetes cluster.
- close this plan after rollout checklist items are executed in target environment.

## Work Items

1. API scaffolding and schema
- create `api/snaplane.molpako.github.io/v1alpha1/backuppolicy_types.go`
- create `api/snaplane.molpako.github.io/v1alpha1/backup_types.go`
- apply kubebuilder validation/default/immutability annotations

2. BackupPolicy controller
- schedule tick and requestedAt triggers
- snapshot create-or-get with metadata patch branches
- `VolumeSnapshot` secondary watch for `readyToUse` transitions
- watch predicates to minimize non-meaningful reconcile
- strict serial dispatcher with `queueTime` ordering and oldest non-`Done` gate
- `Pending -> Dispatched` CAS transition
- failed-head blocking (`A failed` then stop `B`)
- retention deletion for Done snapshots

3. Backup controller
- resolve `spec.volumeSnapshotRef`
- execute transfer job lifecycle
- update `status.conditions`, timestamps, and byte stats
- no queue label mutation in this controller

4. Shared utilities
- queue sort and oldest selection helper
- CAS metadata patch helper
- condition helper

5. Tests
- envtest: schedule->snapshot-created(wait)->ready->dispatch->done
- envtest: `A running` while `B snapshot` exists (no `Backup_B`)
- envtest: `A failed` blocks `B`, manual retry success unblocks
- envtest: create/get patch branches (state empty, Pending missing metadata, terminal states no-patch, foreign policy no-patch)
- envtest: restart idempotency and no duplicate backup creation
- e2e tests are executed only via `-tags=e2e`

## Exit Criteria

- All locked API fields exist with schema validation
- Controller behavior matches `controller-reconcile-contract.md` (A-2 topology)
- Tests cover queue invariants and conflict retry
- Docs and implementation are consistent for v1alpha1

## Verification Commands

- `go test ./...`
- `./bin/kustomize build config/default`
- `make test-e2e-fast`
- `make test-e2e-nightly`

## Release Checklist (v1alpha1)

- cert-manager issuer/secret names validated for target cluster.
- writer DaemonSet scheduled only on `backup-target=true` nodes.
- Lease heartbeat values (`renewTime`, endpoint, used/available bytes) observed in operator namespace.
- backup-controller mTLS secrets mounted and handshake verified.
- failure-path smoke checks:
  - missing/stale assigned node -> `AssignedNodeUnavailable`
  - writer endpoint connection failure -> `WriterEndpointNotReady`
