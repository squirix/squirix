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
u8  opcode           // Put=1, Remove=2, RemoveExpiration=3, TouchExpiration=4, IdempotencyOutcome=5
u16 namespaceLen
u16 keyLen
u32 payloadLen       // Put: cache-entry blob length (CacheEntryCodec); TouchExpiration: 8; IdempotencyOutcome: structured payload; else 0
[namespace utf8]
[key utf8]
[payload bytes]      // Put: binary cache-entry blob (see snapshot-format.md)
```

TouchExpiration payload is `i64 expiresUnixMs` (8 bytes) when `payloadLen = 8`.
