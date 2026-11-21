# Binary journal on-disk format

Journal segments use the `.jsqx` extension with a fixed file header and length-prefixed frames.

## File header

| Offset | Size | Value        |
|--------|------|--------------|
| 0      | 4    | ASCII `SJRN` |
| 4      | 1    | `5`          |

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
u8  opcode           // Put=1, Remove=2, IdempotencyOutcome=3, PutWithMutationOperationId=4, RemoveWithMutationOperationId=5, IdempotencyStarted=6
u16 namespaceLen
u16 keyLen
u32 payloadLen       // per opcode, see below
[namespace utf8]
[key utf8]
[payload bytes]      // Put: binary cache-entry blob (see snapshot-format.md)
```

## Payload by opcode

| Opcode | Name                          | Payload                                                 |
|--------|-------------------------------|---------------------------------------------------------|
| 1      | Put                           | cache-entry blob (`CacheEntryCodec`)                    |
| 2      | Remove                        | empty (`payloadLen = 0`)                                |
| 3      | IdempotencyOutcome            | structured idempotency outcome                          |
| 4      | PutWithMutationOperationId    | mutation prefix, then the cache-entry blob              |
| 5      | RemoveWithMutationOperationId | mutation prefix only                                    |
| 6      | IdempotencyStarted            | structured write-ahead idempotency intent               |

New records use opcodes 1 and 2. Recovery reads older opcodes 3 through 6. The mutation prefix of opcodes 4
and 5 is `u16 operationIdLen`, the UTF-8 operation ID, `u16 fingerprintLen`, and the UTF-8 fingerprint. Recovery
applies the cache mutation from those frames and skips the historical outcome and intent records.

## Cache-entry frames

Every cache-entry frame is either a Put of the complete resulting entry (value, version, tags, and the absolute expiration deadline at
whole-millisecond precision or none) or a Remove. A touch, clearing an entry's TTL with `RemoveExpirationAsync`, or updating an entry journals a Put of the entry it
decided, so no frame depends on how an earlier frame replayed.

Replay applies frames in order. A `Remove` deletes the key regardless of its expiration deadline. With local-clock
expiration (replication factor 1), an expired `Put` also removes the key, superseding any earlier value. With
replicated expiration, recovery preserves the entry even after its deadline. The leader removes it through a
replicated mutation. Recovering from a snapshot and replaying the remaining frames follows the same rules as
replaying the journal.

## File format version

The file header version is `4`. A segment with any other non-zero version under the journal magic is rejected on read and recovery
with an error, and is never truncated or repaired. An opcode value outside the table above fails decoding.
