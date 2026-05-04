# TODO

## Runtime Gaps

- Implement a real SnapshotMetadata-backed range provider and data reader for Ceph-backed incremental backups.
- Replace the hostpath-based nightly SnapshotMetadata lane with a Ceph-backed nightly CBT gate.
- Add operational controls and observability for CAS compaction maintenance.
- Add an operational rollout and runbook for writer-sidecar and cert-manager rotation on a real cluster.
- Add a real-cluster e2e gate for writer heartbeats, mTLS, and restore workflows.

## Repository Semantics

- Make physical CAS repository pruning dependency-safe so a kept manifest never loses a reachable parent.
- Define exactly when `freed` chunk state is emitted during backup generation.
- Define fencing between backup publish and GC mark/sweep so live chunks cannot be deleted.
- Define the repository pruning plan that deletes manifest directories; current compaction only rewrites indexes and packs for manifests already present on disk.
- Document forward and backward compatibility rules for repository and segment versions.
- Strengthen restore-time integrity checks across manifests, segments, indexes, and pack payloads.

## API And Controller Follow-ups

- Decide whether `Backup.status` needs additional operator-facing conditions beyond the current summary model.
- Review whether restore-reference protection in retention needs broader coverage than active PVC restore flows.
- Decide how much manual retry behavior should remain user-driven versus becoming an explicit controller API.
