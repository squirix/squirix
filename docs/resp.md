# RESP cache endpoint

Squirix exposes a RESP2 and RESP3 endpoint over TCP at `127.0.0.1:6379` by default.
It serves single-key cache operations using the same routing, replication, and storage
as the Squirix client endpoint. The receiving node routes each command to the owner or leader of its key.
In a replicated cluster, write replies follow the Squirix replication commit policy.
If the server cannot establish whether a write took effect, it returns `ERR commit outcome unknown`.
Check the current cache state before deciding whether to repeat the write.

## Configure the listener

The Redis client connects in standalone mode. Squirix handles cluster routing internally.
Set `Squirix.Resp` in `appsettings.json` to override the defaults.
A discovered `appsettings.json` can contain RESP settings without a `Squirix.Cluster` section.

```json
{
  "Squirix": {
    "Resp": {
      "Enabled": true,
      "BindAddress": "127.0.0.1",
      "Port": 6379
    }
  }
}
```

| Setting | Environment override | Default |
| --- | --- | --- |
| `Enabled` | `SQUIRIX_RESP_ENABLED` | `true` |
| `BindAddress` | `SQUIRIX_RESP_BIND_ADDRESS` | `127.0.0.1` |
| `Port` | `SQUIRIX_RESP_PORT` | `6379` |

The host applies the environment overrides before validating its effective settings. The `validate-config`
command checks the RESP settings in the supplied file. An explicit `--settings` file requires `Squirix.Cluster`.

Use a literal IP address and a TCP port distinct from the Squirix client and management endpoints.
If multiple nodes share the same bind address, give each a different RESP port or use `0` to let the OS
allocate a port. The host logs a `redis://` URI for TCP or a `rediss://` URI for TLS.

Set `Squirix.Resp.Enabled` to `false` or set `SQUIRIX_RESP_ENABLED=false` to disable the listener on a standalone node.
For an embedded node, set `Resp = new RespOptions { Enabled = false }`.

The defaults also apply when calling `SquirixServer.StartAsync` with embedded server options.

```csharp
using System;
using Squirix.Server;

var options = new SquirixServerOptions
{
    NodeId = "node-a",
    Uri = new Uri("tcp://localhost:5001"),
};
await using var server = await SquirixServer.StartAsync(options);
Console.WriteLine(server.RespUri);
```

## Connect a client

With default settings, run `redis-cli -h 127.0.0.1 -p 6379 PING` to receive `PONG`.

Start the host with `squirix-server run --tls` to encrypt RESP and binary client connections. In this mode,
the RESP listener uses the [server TLS certificate](configuration.md#environment-variables).
The endpoint name used by the client must be covered by that certificate. When
[JWT authentication](security/jwt-signing-keys.md) is configured, pass a valid JWT as the Redis password.
The accepted username is `default`. Configure JWT authentication before binding to a non-loopback address.
Loopback development nodes can use RESP without a token when JWT authentication is disabled.
When JWT is enabled, the client has five seconds after connection setup to authenticate. Failed commands do not extend that window.

Use a PEM file that the client trusts and set `REDISCLI_AUTH` to the current JWT for an authenticated node.

```bash
redis-cli --tls --cacert ./server-ca.pem -h localhost -p 6379 -2 PING
redis-cli --tls --cacert ./server-ca.pem -h localhost -p 6379 -3 SET example value EX 60
redis-cli --tls --cacert ./server-ca.pem -h localhost -p 6379 -3 GET example
```

For StackExchange.Redis with TLS enabled, configure the endpoint, TLS, and the current token before connecting.
The server certificate must be trusted by the client process.

```csharp
using StackExchange.Redis;

var options = new ConfigurationOptions
{
    EndPoints = { "localhost:6379" },
    Ssl = true,
    Protocol = RedisProtocol.Resp3,
    User = "default",
    Password = jwt,
};
await using var connection = await ConnectionMultiplexer.ConnectAsync(options);
var cache = connection.GetDatabase(0);
```

Reconnect with a renewed JWT when the token expires.

## Commands and data

| Purpose | Commands |
| --- | --- |
| Session | `PING`, `ECHO`, `HELLO 2`, `HELLO 3`, `AUTH`, `SELECT 0` |
| Metadata | `INFO`, `CLIENT ID`, `CLIENT GETNAME`, `CLIENT SETNAME`, `CLIENT SETINFO` |
| Read and remove | `GET key`, `EXISTS key`, `DEL key`, `UNLINK key` |
| Write | `SET key value [EX seconds \| PX milliseconds] [NX \| XX]`, `SETEX`, `PSETEX`, `SETNX` |
| Expiration | `EXPIRE`, `PEXPIRE`, `TTL`, `PTTL`, `PERSIST` |

RESP2 is the initial protocol. Clients can negotiate RESP3 with `HELLO 3`. The reply reports the Squirix version.
A connection executes
pipelined commands and returns replies in request order. Separate connections can run concurrently.
`UNLINK` removes the single key through the same path as `DEL`.

Database 0 maps to the named Squirix cache `resp`. Use that exact cache name to read its binary
values through the Squirix SDK. Keys must be valid UTF-8 and satisfy the [Squirix key rules](api.md#cache-keys).
Values can contain arbitrary bytes. The listener accepts up to eight arguments per request, 1 MiB per bulk string,
and 1 MiB plus 16 KiB per request. It admits up to 64 concurrent connections and closes a connection if the next
request takes more than two minutes to arrive. The existing cache admission limits also apply. The default typed SDK serializer writes
`byte[]` as JSON, which RESP `GET` returns as `WRONGTYPE`. Use RESP writes for opaque bytes when sharing this cache
with the SDK. See the [Squirix value encoding rules](api.md#value-encoding).

Hash commands, multi-key commands, expiry condition flags, additional `SET` options, Redis Cluster, Pub/Sub,
and scripting are outside the supported command set.

## Docker

For a single-node container with JWT and a trusted certificate configured, insert these options
before the image name in the [standalone Docker command](containerization.md#run).

```bash
-p 127.0.0.1:6379:6379 \
-e SQUIRIX_RESP_BIND_ADDRESS=0.0.0.0 \
```

Use a different host port, for example `-p 127.0.0.1:6380:6379`, to run alongside Redis.
