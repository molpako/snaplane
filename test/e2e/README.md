# E2E Test Lanes

Snaplane e2e tests use `kubernetes-sigs/e2e-framework` and currently run in two lanes.

## Fast Lane

- command: `make test-e2e-fast`
- cluster: ephemeral Kind cluster created by the test suite
- TLS: `E2E_TLS_MODE=static`
- cert-manager: not required
- purpose: quick validation of scheduler and core controller behavior

## Nightly Lane

- command: `make test-e2e-nightly`
- cluster: fresh Kind cluster recreated by the Make target (`KIND_CLUSTER`, default `kind`)
- TLS: `E2E_TLS_MODE=cert-manager`
- cert-manager: required
- storage path: real `csi-driver-host-path` plus `external-snapshot-metadata`
- controller settings:
  - `SNAPLANE_CBT_PROVIDER=snapshot-metadata`
  - `SNAPLANE_SNAPSHOT_DATA_MODE=live`
- purpose: validate the current hostpath-based SnapshotMetadata integration path

## Ceph Nightly Gate

- command: `make test-e2e-ceph-nightly IMG=<pullable-image>`
- cluster: current kubeconfig with Ceph CSI RBD and SnapshotMetadataService already installed
- storage path: `ceph-csi` RBD plus `external-snapshot-metadata`
- configuration:
  - `E2E_STORAGE_CLASS` selects the Ceph RBD `StorageClass`
  - `E2E_VOLUME_SNAPSHOT_CLASS` selects the Ceph RBD `VolumeSnapshotClass`
  - scheduled CI defaults to `csi-rbd-sc` and `csi-rbdplugin-snapclass`
  - `E2E_USE_REAL_CBT_PROVIDER=true`
- purpose: validate the production-intended CBT semantics and live incremental backup path
- CI entrypoint: `.github/workflows/e2e-ceph-nightly.yaml`

## Real Cluster Gate

- command: `make test-e2e-real-cluster IMG=<locally-built-image>`
- cluster: workflow-created minikube `none` driver cluster
- TLS: `E2E_TLS_MODE=cert-manager`
- cert-manager: required
- image: the workflow builds `IMG=controller:latest` on the runner Docker daemon from the checked-out source before running the gate
- configuration:
  - `E2E_STORAGE_CLASS=csi-hostpath-sc`
  - `E2E_VOLUME_SNAPSHOT_CLASS=csi-hostpath-snapclass`
- coverage:
  - writer Lease heartbeats through the writer-sidecar pod
  - controller-to-writer mTLS by completing a backup after endpoint recovery
  - restore worker path by copying a seeded mock backup into a block-mode `pvcPrime`
- CI entrypoint: `.github/workflows/e2e-real-cluster.yaml`

## Notes

- the Make targets use `-count=1` to avoid stale `go test` cache results
- e2e coverage should prefer real integrations over mocks
- the hostpath nightly lane remains useful for dependency smoke coverage; the Ceph nightly gate is the production-like CBT lane
