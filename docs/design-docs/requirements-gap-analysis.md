# Requirements Gap Analysis

## Summary

Current direction is coherent, but several requirements are still implicit.
Those must be written explicitly before implementation starts.

## Missing or Under-specified Requirements

1. Snapshot pair validity checks
  - Ensure base and target snapshots belong to the same source volume.
  - Ensure target is newer than base for delta operations.
  - Define behavior for invalid pair requests.

2. CBT stream resume semantics
  - Define operator behavior for starting_offset resume.
  - Define idempotent retry model for interrupted streams.
  - Define max_results policy and controller memory bounds.

3. Feature gating and compatibility
  - Explicitly gate CBT usage on SnapshotMetadata support.
  - Define fallback behavior when RBDSnapDiffByID is unavailable.
  - Define required image feature policy (exclusive-lock/object-map/fast-diff).

4. Encryption and offset correctness
  - Define behavior when volume uses LUKS header padding.
  - Ensure offsets map to user data correctly during backup/restore.
  - Decide whether encrypted-on-disk bytes or logical user blocks are backed up.

5. Manifest chain management
  - Define maximum chain depth target and merge strategy.
  - Define optional synthetic full checkpoints.
  - Define restore SLA for deep chains.

6. Repository write atomicity
  - Specify pack/index/manifest write ordering.
  - Define crash recovery scan and repair procedure.
  - Define lock timeout and stale lock reclaim policy.

7. Data integrity and verification
  - Define checksum strategy for chunk payload and manifests.
  - Define periodic scrub workflow.
  - Define restore-time verification level.

8. Node locality and failure domain
  - Define behavior when scheduled node differs from repository node.
  - Define disk full and disk corruption handling.
  - Define disaster story for node loss.

9. Retention edge cases
  - Define behavior when N-day policy deletes parent needed by a kept delta.
  - Define whether parent re-linking or synthetic base creation is required.
  - Define race behavior between backup write and retention prune.

10. Security and multi-tenancy
  - Define secret handling for SnapshotMetadata calls.
  - Define repository path isolation across namespaces/tenants.
  - Define least-privilege FS permissions.

## Evidence Notes

- SnapshotMetadata request/response semantics:
  - /Users/molpako/src/github.com/ceph/ceph-csi/vendor/github.com/container-storage-interface/spec/lib/go/csi/csi.pb.go:5902
- ceph-csi SMS request validation and limits:
  - /Users/molpako/src/github.com/ceph/ceph-csi/internal/rbd/sms_controllerserver.go:62
- ceph-csi diff processing with LUKS header adjustment:
  - /Users/molpako/src/github.com/ceph/ceph-csi/internal/rbd/snap_diff.go:78
- local csi-addons spec clone currently lacks SnapshotMetadata proto surface:
  - /Users/molpako/src/github.com/csi-addons/spec

