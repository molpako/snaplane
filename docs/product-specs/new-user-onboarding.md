# New User Onboarding (Draft)

## Goal

Help a platform user create reliable periodic RBD backups with minimal steps.

## First-Time Flow

1. Create BackupRepository that points to a node-local path.
2. Create BackupPolicy bound to target PVC/VolumeSnapshot source.
3. Observe first successful base backup.
4. Verify restore by creating a test PVC with `dataSourceRef -> Backup` and checking volume content.

## Expected UX Signals

- clear status phases
- explicit failure reason on auth, snapshot, IO, and quota errors
- retention and GC outcomes visible in status
