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

## Planned CSI Backend CBT Gate

- status: not yet implemented
- intended storage path: a real CSI driver with CBT support plus `external-snapshot-metadata`
- purpose: validate production-intended CBT semantics and the live incremental backup path

## Notes

- the Make targets use `-count=1` to avoid stale `go test` cache results
- e2e coverage should prefer real integrations over mocks
- the current nightly lane is not the final production-like CBT gate; that role is reserved for the planned CSI backend lane
