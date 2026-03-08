# chunk-pack-mini example

This example shows how a changed chunk is handled in delta backup.

- `before/`: base snapshot (A) with packs `000001..000004`
- `after/`: delta snapshot (B) adds `pack-000005.pack`

Key point:
- old chunk (`ch_slot420_oldx`) in `pack-000002.pack` is NOT overwritten
- new chunk (`ch_slot420_newx`) is appended as new data in `pack-000005.pack`
- delta manifest only updates `changedSlots[420]`

Pack format in this example (demo only):
- 28-byte header: `CID:<chunk-id-15bytes>;LEN:100;`
- 100-byte payload
- total per record: 128 bytes

Real implementation note:
- chunk IDs should be cryptographic hashes
- idx should be generated from durable append transactions
