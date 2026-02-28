# GC Strategy

## Problem

After retention deletes manifests, unreferenced chunks remain in packs.
Without GC, disk usage only grows.

## Constraints

- packs are append-oriented
- multiple manifests can reference same chunk
- node-local storage has finite capacity
- backup and restore should remain available during GC

## Option A: Online Reference Counting

Description:
- increment/decrement refcount on every manifest write/delete

Pros:
- immediate reclaim eligibility tracking

Cons:
- fragile under crashes
- expensive correction path when counters drift
- high write amplification

## Option B: Epoch Mark-and-Sweep (Recommended v1)

Description:
- build live set by traversing retained manifests
- mark reachable chunk IDs
- sweep index entries not in live set
- compact packs in follow-up step

Pros:
- robust and simple correctness model
- easy recovery after interruption
- no per-write refcount churn

Cons:
- periodic full scan cost
- delayed reclaim

## Option C: Copy-forward Compaction Only

Description:
- no explicit mark phase; rewrite live chunks opportunistically

Pros:
- compact final state

Cons:
- high IO cost
- hard to bound runtime

## Recommended v1 Workflow

1. freeze retention candidate set
2. traverse retained manifests and mark chunk IDs
3. emit gc-live index snapshot
4. sweep stale index entries
5. compact packs only when fragmentation threshold exceeds limit
6. publish new index generation atomically

## Safety Rules

- do not delete source pack blocks before new compacted pack index is durable
- keep last successful index generation as rollback checkpoint
- GC runs with exclusive maintenance lock or isolated generation namespace

## Metrics

- mark scanned manifests
- live chunk count
- reclaimed bytes
- compaction bytes moved
- gc duration and interruption count

