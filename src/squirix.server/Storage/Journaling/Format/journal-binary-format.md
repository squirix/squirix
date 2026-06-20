# Pipelined binary on-disk format

Segment files use the same `.jsqx` naming as JsonFramed journal. The file header matches JsonFramed (`SJRN` version `1`); frame **body** encoding discriminates backends.

## File header

| Offset | Size | Value        |
|--------|------|--------------|
| 0      | 4    | ASCII `SJRN` |
| 4      | 1    | `1`          |

Readers probe the first frame body after the header: UTF-8 JSON object (`{` …) → JsonFramed; otherwise → Pipelined binary body layout below.

## Frame layout (little-endian)

```
u32 frameLength      // body length only
[body bytes]
u32 crc32c           // CRC32C over body
```

### Binary body (Pipelined)

```
u64 sequence
i64 unixMs
u8  opcode           // Put=1, Remove=2, RemoveExpiration=3, TouchExpiration=4
u16 namespaceLen
u16 keyLen
u16 opIdLen          // Put only; 0 otherwise
u32 payloadLen       // Put: discriminated entry JSON length; TouchExpiration: 8; else 0
[namespace utf8]
[key utf8]
[payload bytes]      // Put discriminated JSON
[opId utf8]          // Put idempotency operation id
```

TouchExpiration payload is `i64 expiresUnixMs` (8 bytes) when `payloadLen = 8`.
