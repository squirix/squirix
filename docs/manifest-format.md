# Manifest on-disk format

Manifest files use the `.bmqx` extension with a fixed file header, typed body, and trailing CRC32C.
The active manifest index is stored in a two-slot `man-current` pointer (magic `SQMP`).

## Numbered manifest file (`.bmqx`)

Filename pattern: `man-NNNNNN.bmqx`.

Retention deletes the files below the index both pointer slots name and keeps that one and every file above it.

### File layout (little-endian)

| Region  | Size     | Value                            |
| ------- | -------- | -------------------------------- |
| magic   | 4        | ASCII `SQMF`                     |
| version | 1        | `1`                              |
| body    | variable | see below                        |
| crc32c  | 4        | CRC32C over `version` + `body`   |

### Body

```text
u32 format
u32 currentJournal
u64 nextSequence
u8  hasSnapshot            // 0 = absent, 1 = present
[if hasSnapshot]
  u32 snapshotIndex
  u64 lastAppliedSequence
  u32 replayFromJournalSegment
  i64 createdUtcUnixMs
  u16 pathLen
  [path utf8 bytes when pathLen > 0]
```

## CURRENT pointer (`man-current`)

Exactly **8192 bytes** (binary blob; not UTF-8 text): two slots of 4096 bytes at offsets 0 and 4096. Each slot starts with
a 21-byte record, little-endian; the rest of the slot is zero and ignored.

| Offset | Size | Value                                                               |
| ------ | ---- | ------------------------------------------------------------------- |
| 0      | 4    | ASCII `SQMP`                                                        |
| 4      | 1    | slot version `1`                                                    |
| 5      | 8    | `u64` sequence, at least 1, increasing with every write             |
| 13     | 4    | `i32` manifest index (1-based, matches numbered file suffix)        |
| 17     | 4    | CRC32C over bytes 0–16                                              |

The file is created and written in place through one handle held for the life of the ledger; no temporary file, rename or
delete is involved.

Publish order: write the numbered `.bmqx` file, flush it and its directory entry, then write the new record into the slot
that does not hold the newest one and flush it. The pointer has moved once that flush returns. The same index is then
written into the other slot with the next sequence, so damage at rest to one slot loses nothing, and the publish is acknowledged only after that
second flush. A crash after the data file but before the pointer update leaves an orphan numbered file; recovery continues to
use the previous pointer.

Selection on read:

- A slot is valid when its magic, version and CRC32C check, its sequence is not zero and its index is positive.
- The valid slot with the higher sequence wins; two valid slots with the same sequence must be byte-identical.
- A missing file, or one whose two slot records hold no non-zero byte, means no manifest was ever published.
- Any other file without a valid slot fails loudly and is left unchanged.
- A pointer written by an earlier build (12 bytes, magic `SQMC`) is refused and left unchanged.

## CRC32C

Castagnoli polynomial (`0x82F63B78` reflected), matching journal frame checksums.
