# Binary snapshot format (`.bsqx`)

On-disk snapshot files use extension `.bsqx` and filename pattern `snp-NNNNNN.bsqx`.

## File layout

| Region  | Size     | Value                                      |
| ------- | -------- | ------------------------------------------ |
| magic   | 4        | ASCII `SQSS`                               |
| version | 1        | `1`                                        |
| records | variable | typed records                              |
| fileCrc | 4        | CRC32C over `version` + all record bytes   |

CRC32C uses the Castagnoli polynomial (`0x82F63B78`, reflected), matching journal and manifest files.

The file stores no total length. Writer and reader address the file with 64-bit offsets, so its size is limited only by
the file system; a single record body is at most 1 GiB. The format version stays `1`. A snapshot is written to a
temporary file next to the published one, so the data directory needs about twice the snapshot size free while a
snapshot is taken.

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
| Object  | 7  | `u16` property count, then repeated name + recursive value              |
| Array   | 8  | `u32` element count, then repeated recursive value                      |

Complex values (`JsonElement`, arbitrary POCOs) are encoded as recursive binary trees (`Object` / `Array`)
on write and materialized as owned `JsonElement` on read.

## Idempotency body

| Field       | Encoding                              |
| ----------- | ------------------------------------- |
| operationId | `u16` length + UTF-8                  |
| fingerprint | `u16` length + UTF-8                  |
| createdUtc  | `i64` Unix milliseconds UTC           |
| outcomeKind | `u16` length + UTF-8 (`insert` today) |

## Publish semantics

Writes go to `snp-NNNNNN.tmp` and are published to `snp-NNNNNN.bsqx` via
`IStorageFileOperations.PublishSnapshot` with a no-overwrite move: an existing target fails the publish and is never
replaced. The index is above both the snapshot the manifest names and every `snp-*.bsqx` file on disk, so an unreferenced
snapshot left by a crash before the manifest update is skipped, not overwritten, and deleted once a later snapshot is
published. Retention keeps the snapshot the manifest names and every snapshot file above its index, and deletes the
files below it. A manifest that names no snapshot makes retention delete none until
the next snapshot is published.

## Replica-group snapshots

Replica-group recovery snapshots are separate from cache snapshots and are stored as two slot files,
`group.snapshot.0` and `group.snapshot.1`, inside the group persistence directory. The snapshot image is:

```text
SQRS | version:u8 | payloadLength:u32 | payload | CRC32C(payload):u32
```

This image is also what a state transfer carries. A slot file wraps one image:

```text
SQGS | slotVersion:u8 | sequence:u64 | imageLength:i32 | image | CRC32C(all preceding bytes):u32
```

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

A slot file is valid when its magic, version, length, and CRC check, its sequence is not zero, and its image decodes.
The slot CRC covers the slot header and the image, so a header persisted over an older image is not mistaken for a
newer snapshot. A slot that fails a frame check is torn and ignored. A slot with an intact frame whose image does not
decode, uses another image version, or is larger than the size bound plus the slot frame is refused as an error, as is
a leftover `group.snapshot` file written by an earlier build. The valid slot with the higher sequence is the
snapshot; two valid slots with one sequence must be byte-identical.

Publication never renames, replaces, or deletes a file. It writes the new snapshot in place into the slot that is not
current and flushes it, then writes it again into the slot that was current and flushes that, so both slots end up
holding the snapshot. Before the first flush returns, a crash leaves either the previous snapshot or the new one; after
it, the new one. When no snapshot exists yet, a publish that stopped after the first slot write leaves slot 0 torn or
valid and slot 1 absent or torn. Recovery validates the slots as above before restoring the committed baseline. Journal compaction keeps
the snapshot published and retains the log header plus entries after `lastIncludedIndex`, so a crash during or after
compaction remains restartable. A replica that fell behind the compacted log is repaired by a state transfer: the
leader sends the published image, the receiving replica checks it before it changes anything, and it publishes the same
snapshot when the transfer completes.
