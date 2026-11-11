# Binary snapshot format (`.bsqx`)

On-disk snapshot files use extension `.bsqx` and filename pattern `snp-NNNNNN.bsqx`.

## File layout

| Region  | Size     | Value                                      |
| ------- | -------- | ------------------------------------------ |
| magic   | 4        | ASCII `SQSS`                               |
| version | 1        | `2`                                        |
| records | variable | typed records                              |
| fileCrc | 4        | CRC32C over `version` + all record bytes   |

CRC32C uses the Castagnoli polynomial (`0x82F63B78`, reflected), matching journal and manifest files.

The file stores no total length. Writer and reader address the file with 64-bit offsets, so its size is limited only by
the file system; a single record body is at most 1 GiB. A reader accepts only the current format version `2`; a snapshot
of any other version stops recovery with an error and is never skipped in favor of the journal. See
[Publish semantics](#publish-semantics) for file creation and the
[runbook](operational-runbook.md#snapshot-artifacts-and-memory-pressure) for disk headroom.

## Record layout

Each record is:

| Field     | Size      | Description                        |
| --------- | --------- | ---------------------------------- |
| kind      | 1         | `1` = entry, `2` = idempotency     |
| recordLen | 4         | body length (little-endian `u32`)  |
| body      | recordLen | payload                            |
| crc32c    | 4         | CRC32C over `body` only            |

## Entry body

| Field     | Encoding                                      |
| --------- | --------------------------------------------- |
| namespace | `u16` UTF-8 byte length + UTF-8 bytes         |
| key       | `u16` UTF-8 byte length + UTF-8 bytes         |
| entry     | cache-entry blob (see below)                  |

## Cache-entry blob

| Field           | Encoding                                           |
| --------------- | -------------------------------------------------- |
| hasExpiresUtc   | `u8` (`0` absent, `1` present)                     |
| expiresUtc      | `i64` Unix milliseconds UTC when present           |
| hasExpiration   | `u8` (`0` absent, `1` present)                     |
| expirationTicks | `i64` `TimeSpan` ticks when present                |
| version         | `i64`                                              |
| tagCount        | `u16`                                              |
| tags            | repeated u16-prefixed UTF-8 key/value pairs        |
| valueKind       | `u8`                                               |
| value           | kind-specific payload                              |

### Value kinds

| Kind    | ID | Payload                                                                 |
| ------- | -- | ----------------------------------------------------------------------- |
| Null    | 0  | (none)                                                                  |
| Bool    | 1  | `u8` (`0`/`1`)                                                          |
| String  | 2  | `u32` length + UTF-8 bytes                                              |
| Bytes   | 3  | `u32` length + raw bytes                                                |
| Int64   | 4  | `i64`                                                                   |
| Double  | 5  | `f64` IEEE little-endian                                                |
| Decimal | 6  | `u16` length + UTF-8 invariant decimal text                             |
| Json    | 7  | `u32` length + one JSON value as UTF-8 text                             |

A structured value (an object, an array, a number that does not fit a primitive kind, an arbitrary POCO) is stored as the
UTF-8 JSON text the client sent, byte for byte: property order, duplicate names, white space and number text are kept, so
every digit of a number survives a restart and a replica. The text is exactly one JSON value without a byte order mark, with
valid UTF-8, no comments, no trailing commas and nesting of at most 64 levels. A reader checks this and refuses an entry that
fails, and it copies the text into its own buffer. Value kinds that are not listed (including `8` and above) are refused.

## Idempotency body

The current writer starts each body with a state byte. All integer fields below are little-endian.

| Field       | Encoding                                  |
| ----------- | ----------------------------------------- |
| state       | `u8`: `0` = completed, `1` = started      |
| operationId | `u16` UTF-8 byte length + UTF-8 bytes     |

A completed record (`state = 0`) continues with:

| Field          | Encoding                              |
| -------------- | ------------------------------------- |
| fingerprint    | `u16` UTF-8 byte length + UTF-8 bytes |
| createdUtc     | `i64` Unix milliseconds UTC           |
| responseLength | `i32`, non-negative                   |
| responseBytes  | `responseLength` raw bytes            |

A started record (`state = 1`) continues with:

| Field          | Encoding                                          |
| -------------- | ------------------------------------------------- |
| hasFingerprint | `u8`: `0` = absent, `1` = present                 |
| fingerprint    | `u16` UTF-8 byte length + UTF-8 bytes, if present |
| createdUtc     | `i64` Unix milliseconds UTC                       |

The reader also accepts the legacy completed-record body without the leading state byte: `operationId`,
`fingerprint`, `createdUtc`, `responseLength`, and `responseBytes` in that order. This record-level compatibility does
not allow a different snapshot file version. The implementation is
[`IdempotencyCodec`](../src/squirix.server/Storage/Snapshot/Binary/IdempotencyCodec.cs).

## Publish semantics

Writes go to `snp-NNNNNN.tmp` and are published to `snp-NNNNNN.bsqx` via
`IStorageFileOperations.PublishSnapshot` with a no-overwrite move: an existing target fails the publish and is never
replaced. The index is above both the snapshot the manifest names and every `snp-*.bsqx` file on disk, so an unreferenced
snapshot left by a crash before the manifest update is skipped, not overwritten, and deleted once a later snapshot is
published. Retention keeps the snapshot the manifest names and every snapshot file above its index, and deletes the
files below it. A manifest that names no snapshot makes retention delete none until
the next snapshot is published.

## Replica-group snapshots

Replica-group recovery snapshots are separate from cache snapshots. Each immutable generation is stored as
`group.checkpoint-<generation>.snapshot` in the group persistence directory. The snapshot image is:

```text
SQRS | version:u8 | payloadLength:u32 | payload | CRC32C(payload):u32
```

This image is also what a state transfer carries. The `group.snapshot.meta` file contains two 4 KiB pointer
slots. Each stores a magic value, a publication sequence, a snapshot generation ID, zero padding, and a CRC32C
over the preceding slot bytes. The empty generation ID represents recovery from the full WAL.

All integer fields are little-endian. The payload begins with five fixed 64-bit fields (configuration generation,
`lastIncludedTerm`, `lastIncludedIndex`, `commitIndex`, and `capturedUtc` in Unix milliseconds), followed by a
length-prefixed UTF-8 `groupId`, the topology fingerprint, and the resolved idempotency outcomes whose journal index is
covered by the snapshot. The default maximum accepted file size is 64 MiB, configurable through
`FollowerLogOptions.MaxSnapshotBytes`.

`capturedUtc` is the time the outcomes were captured, on the clock that stamped their resolution times. A node that
restores the snapshot takes the age of each outcome as `capturedUtc` minus its resolution time and keeps counting it on
its own monotonic clock, so the retention window never depends on how far apart two node clocks are. Time the snapshot
spends at rest or in transit does not count: a restored outcome may stay replayable longer than its window, never
shorter, and only while no newer outcome needs its place. A snapshot carrying an outcome resolved after `capturedUtc` is refused as corrupt.

Recovery checks the pointer slots and tries their referenced snapshots in descending publication order. A torn
pointer or damaged snapshot can fall back to the preceding generation. Local compaction flushes and verifies the
new snapshot before publishing its pointer. It retains the WAL from the preceding snapshot boundary so either
generation can recover every committed index. The first compaction retains the full WAL for the same reason.

State transfer writes two independent copies at the incoming boundary before replacing the old WAL. The old
snapshot file formats are rejected.
