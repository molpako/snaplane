# Core Beliefs

## Product Beliefs

- Backups must be restorable before they are considered successful.
- Node-local repository behavior must be explicit about node affinity and failure domains.
- Incremental optimization must not compromise deterministic restore correctness.

## Data Beliefs

- Repository chunking spec is immutable per repository.
- Metadata integrity is as important as data integrity.
- Sparse/zero semantics must be represented explicitly.

## Operator Beliefs

- Reconcile loops must be idempotent and restart-safe.
- APIs should expose intent, not implementation internals.
- Long-running IO work needs resumability and bounded retries.

## Operational Beliefs

- Default choices should be safe, not only fast.
- Retention and GC must be transparent and explainable.
- Every destructive operation must have an audit trail.

