# Binary journal on-disk format

Journal segments use the `.jsqx` extension with a fixed file header and length-prefixed frames.

## File header

| Offset | Size | Value        |
|--------|------|--------------|
| 0      | 4    | ASCII `SJRN` |
| 4      | 1    | `1`          |

## Frame layout (little-endian)

```text
u32 frameLength      // body length only
[body bytes]
u32 crc32c           // CRC32C over body
```

### Binary frame body

```text
u64 sequence
i64 unixMs
u8  opcode           // Put=1, Remove=2, IdempotencyOutcome=5, PutWithMutationOperationId=6, RemoveWithMutationOperationId=7, IdempotencyStarted=10
u16 namespaceLen
u16 keyLen
u32 payloadLen       // per opcode, see below
[namespace utf8]
[key utf8]
[payload bytes]      // Put: binary cache-entry blob (see snapshot-format.md)
```

## Payload by opcode

| Opcode | Name                          | Payload                                                       |
|--------|-------------------------------|---------------------------------------------------------------|
| 1      | Put                           | cache-entry blob (`CacheEntryCodec`)                          |
| 2      | Remove                        | empty (`payloadLen = 0`)                                      |
| 5      | IdempotencyOutcome            | structured idempotency outcome                                |
| 6      | PutWithMutationOperationId    | mutation operation id prefix, then the cache-entry blob       |
| 7      | RemoveWithMutationOperationId | mutation operation id prefix only                             |
| 10     | IdempotencyStarted            | structured write-ahead idempotency intent                     |

## Cache-entry frames

Every cache-entry frame is either a Put of the complete resulting entry (value, version, tags, and the absolute expiration deadline at
whole-millisecond precision or none) or a Remove. A touch, an expiration removal or an update of an entry journals a Put of the entry it
decided, so no frame depends on how an earlier frame replayed.

Replay keeps, for each key, the state of its last frame, and drops the key only when the deadline of that final entry has passed. Compaction
and snapshot plus tail recovery give the same state for any cut of the journal.

## Retired opcodes

Opcode values 3, 4, 8 and 9 belonged to the former touch-expiration and remove-expiration frames. They are retired and never reused. A frame
carrying a retired or otherwise unassigned opcode fails recovery.
