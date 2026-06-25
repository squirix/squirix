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
