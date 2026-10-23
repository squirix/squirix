# Manifest on-disk format

The manifest lives in one file, `man-current`: two slots, each holding the whole manifest. There are no numbered manifest files.

## `man-current`

Exactly **8192 bytes** (binary blob; not UTF-8 text): two slots of 4096 bytes at offsets 0 and 4096. Each slot starts with
a 64-byte record, little-endian; the rest of the slot is zero and ignored. The record fits the first sector of its slot, and a
write to one slot never touches the other.

| Offset | Size | Value                                                             |
| ------ | ---- | ----------------------------------------------------------------- |
| 0      | 4    | ASCII `SQMP`                                                      |
| 4      | 1    | slot version `2`                                                  |
| 5      | 8    | `u64` sequence, at least 1, increasing with every write           |
| 13     | 4    | `i32` current journal segment, at least 1                         |
| 17     | 8    | `u64` next journal sequence, at least 1                           |
| 25     | 1    | `u8` has snapshot, `0` or `1`                                     |
| 26     | 4    | `i32` snapshot index, at least 1 when present, otherwise `0`      |
| 30     | 8    | `u64` snapshot last applied sequence, `0` when absent             |
| 38     | 4    | `i32` replay-from journal segment, `0` when absent                |
| 42     | 8    | `i64` snapshot creation time as UTC ticks, `0` when absent        |
| 50     | 10   | zero                                                              |
| 60     | 4    | CRC32C over bytes 0–59                                            |

The snapshot file is not named in the record: it is derived from the snapshot index as `snp-NNNNNN.bsqx` in the data
directory.

The file is created and written in place through one handle held for the life of the ledger; no temporary file, rename or
delete is involved.

Publish order: write the new manifest into the slot that does not hold the newest one and flush it. The manifest has moved once
that flush returns. The same manifest is then written into the other slot with the next sequence, so damage at rest to one slot
loses nothing, and the publish is acknowledged only after that second flush. Retention of snapshots and journal segments runs only
after the second flush, so neither slot ever holds a manifest whose snapshot or segments were deleted.

A crash or torn write during the first flush leaves the other slot holding the whole previous manifest; recovery continues from it.

Selection on read:

- A slot is valid when its magic, version and CRC32C check, its sequence is not zero, and its fields follow the rules above
  (a violated rule makes the slot torn).
- The valid slot with the higher sequence wins; two valid slots with the same sequence must be byte-identical.
- A missing file, or one whose two slot records hold no non-zero byte, means no manifest was ever published; the default
  manifest (journal segment 1, next sequence 1, no snapshot) applies.
- Any other file without a valid slot fails loudly and is left unchanged.
- A file written by an earlier build (magic `SQMC`, or magic `SQMP` with slot version `1`) is refused and left unchanged.

## CRC32C

Castagnoli polynomial (`0x82F63B78` reflected), matching journal frame checksums.
