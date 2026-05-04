# Writer Sidecar And Certificate Rotation Runbook

## Scope

This runbook covers the operational rollout path for the writer sidecar and its
mTLS material in a real cluster.

## Preflight

- confirm cert-manager is installed and its webhook is ready
- confirm the Snaplane namespace contains `writer-ca`, `writer-client-cert`, and
  `writer-sidecar-server-cert`
- confirm backup target nodes have
  `snaplane.molpako.github.io/backup-target=true`
- confirm writer Leases are fresh and expose:
  - `snaplane.molpako.github.io/writer-endpoint`
  - `snaplane.molpako.github.io/used-bytes`
  - `snaplane.molpako.github.io/available-bytes`

## Rollout

1. Apply the updated manifests.
2. Wait for cert-manager to issue or renew all writer certificates.
3. Restart writer-sidecar pods after server certificate rotation.
4. Restart the controller manager after client certificate rotation.
5. Verify writer Leases refresh within one heartbeat interval.
6. Run one manual `BackupPolicy.spec.manual.requestID` and confirm the created
   `Backup` reaches `Succeeded=True`.
7. Create a restore PVC with `dataSourceRef -> Backup` and confirm the restore
   populator completes and removes its protection finalizer.

## Rotation Checks

- server certificate SANs must cover the service DNS names used by the writer
  endpoint path
- writer sidecar must require and verify client certificates
- controller client certificate must chain to `writer-ca`
- stale writer Leases should age out before scheduling chooses the node again

## CAS Maintenance Checks

When CAS mode is enabled, inspect writer Lease annotations after maintenance:

- `snaplane.molpako.github.io/cas-maintenance-last-run`
- `snaplane.molpako.github.io/cas-maintenance-last-error`
- `snaplane.molpako.github.io/cas-maintenance-repos`
- `snaplane.molpako.github.io/cas-maintenance-reclaimed-bytes`

Set `-cas-maintenance-interval=0` only during controlled investigation. Restore
the normal interval before returning the node to regular backup duty.
