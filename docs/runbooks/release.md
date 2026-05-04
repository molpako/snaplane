# Release Runbook

## Scope

This runbook covers the release-candidate validation path before creating a
Snaplane release.

## GitHub Setup

No external cluster credentials are required for the `E2E Real Cluster`
workflow. The workflow creates a minikube qemu cluster and enables the required
CSI hostpath and VolumeSnapshot addons before running the gate.

## Preflight

1. Confirm the release branch is based on the intended commit.
2. Confirm normal CI is green.
3. Confirm the `E2E Real Cluster` workflow will run from the intended commit.

## Real-Cluster Gate

1. Open the `E2E Real Cluster` workflow.
2. Run the workflow manually.
3. Confirm the workflow completes successfully.

This gate validates the real-cluster writer, cert-manager TLS, backup retry, and
restore worker paths using an image built from the workflow checkout.

## Ceph Gate

Before tagging a release, confirm one of the following:

- the latest scheduled `E2E Ceph Nightly` run passed for the release candidate
  or an equivalent commit
- the `E2E Ceph Nightly` workflow was manually re-run with the release-candidate
  image, Ceph RBD StorageClass, and Ceph RBD VolumeSnapshotClass

The Ceph gate requires a cluster with Ceph CSI and SnapshotMetadataService
already installed. Use the workflow diagnostics artifact when it fails.

## Release Evidence

Record the following in the release notes or release checklist:

- release commit SHA
- `E2E Real Cluster` workflow run URL
- `E2E Ceph Nightly` workflow run URL or accepted latest nightly run
- any known external-cluster caveats

## Tagging

Create the release tag only after the required release gates pass or an explicit
release decision records why a gate was waived.
