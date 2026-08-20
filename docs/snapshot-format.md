# Binary snapshot format (`.bsqx`)

On-disk snapshot files use extension `.bsqx` and filename pattern `sn-NNNNNN.bsqx`.

## File layout

| Region  | Size     | Value                                      |
| ------- | -------- | ------------------------------------------ |
| magic   | 4        | ASCII `SQSS`                               |
| version | 1        | `1`                                        |
| records | variable | typed records                              |
| fileCrc | 4        | CRC32C over `version` + all record bytes   |

CRC32C uses the Castagnoli polynomial (`0x82F63B78`, reflected), matching journal and manifest files.

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

Writes go to `sn-NNNNNN.tmp` and are published atomically to `sn-NNNNNN.bsqx` via
`IStorageFileOperations.PublishSnapshot`.

## Replica-group snapshots

Replica-group recovery snapshots are separate from cache snapshots and are stored as `group.snapshot` inside the
group persistence directory. The format is:

```text
SQRS | version:u8 | payloadLength:u32 | payload | CRC32C(payload):u32
```

All integer fields are little-endian. The payload begins with a length-prefixed UTF-8 `groupId` encoded from
`snapshot.GroupId` by `GroupSnapshotStore`, followed by the topology fingerprint, configuration generation,
`lastIncludedTerm`, `lastIncludedIndex`, `commitIndex`, and resolved idempotency outcomes whose journal index is
covered by the snapshot. The default maximum accepted file size is 64 MiB, configurable through
`FollowerLogOptions.MaxSnapshotBytes`.

Publication writes `group.snapshot.tmp`, flushes it, and atomically replaces `group.snapshot`. Recovery validates the
magic, version, declared length, size bound, and CRC before restoring the committed baseline. Journal compaction keeps
the snapshot published and retains the log header plus entries after `lastIncludedIndex`, so a crash during or after
compaction remains restartable and the snapshot remains installable by a lagging replica.
