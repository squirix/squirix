# Binary manifest on-disk format

Manifest files use the `.bmqx` extension with a fixed file header, typed body, and trailing CRC32C.
The active manifest index is stored in a fixed-size binary `man-current` pointer (magic `SQMC`).

Production default remains JSON manifests (`ManifestBackend.Json`); see [persistence](persistence.md).

## Numbered manifest file (`.bmqx`)

Filename pattern: `man-NNNNNN.bmqx` (same `man-` prefix as JSON manifests, distinct extension).

### File layout (little-endian)

| Region | Size | Value |
|--------|------|-------|
| magic | 4 | ASCII `SQMF` |
| version | 1 | `1` |
| body | variable | see below |
| crc32c | 4 | CRC32C over `version` + `body` |

### Body

```
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

Fixed **12 bytes** (binary blob; not UTF-8 text):

| Offset | Size | Value |
|--------|------|-------|
| 0 | 4 | ASCII `SQMC` |
| 4 | 4 | `u32` manifest index (1-based, matches numbered file suffix) |
| 8 | 4 | CRC32C over bytes 0–7 |

Publish order: write the numbered `.bmqx` file and flush it to disk, then overwrite `man-current` in place (write-through on Windows). A crash after the data file but before the pointer update leaves an orphan numbered file; recovery continues to use the previous pointer.

## CRC32C

Castagnoli polynomial (`0x82F63B78` reflected), matching journal frame checksums.
