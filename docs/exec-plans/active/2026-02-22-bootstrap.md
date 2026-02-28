# Active Exec Plan: Design Bootstrap

Date: 2026-02-22
Status: active

## Objective

Finalize documentation-first architecture for the RBD local backup operator.

## Work Items

1. Confirm manifest v1 format
- pick exact segment encoding
- define checksum and version fields

2. Confirm GC v1 workflow
- define mark source and sweep atomics
- define compaction thresholds

3. Locking and crash recovery spec
- write repo lock contract
- define restart recovery flowchart

4. CRD/API draft
- define minimal spec/status schema per CRD
- define condition set and terminal reasons

5. SLO and non-functional requirements
- define backup/restore timing classes
- define observability acceptance criteria

## Exit Criteria

- unresolved design decisions reduced to implementation-ready level
- each decision has owner, rationale, and compatibility notes

