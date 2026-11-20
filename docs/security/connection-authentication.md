# Cache connection authentication

When JWT authentication is configured, Squirix authenticates each physical external TLS connection before accepting cache RPCs on it.
The SDK obtains a JWT from `SquirixClientOptions.BearerTokenProvider` during that connection's authentication exchange.
Subsequent RPCs use the identity bound to the connection and do not send a bearer token or repeat its signature verification.
Authorization, the accepted lifetime bounds and the active trust configuration are still checked at each request boundary.

Update SDK and server together. The client presents its bearer token during the binary TLS connection handshake.

## Supply credentials

Set `BearerTokenProvider` to supply a JWT for each new physical connection.
The provider must return a currently valid token and support cancellation and concurrent invocations.
A client may connect to multiple endpoints. Reconnect or failover can invoke the provider again.
Transports can also rotate before token expiry; the provider must remain available for the client lifetime.

Cache identity-provider acquisitions inside your provider when appropriate, but refresh expiring credentials.
Returning the same expired string on a new connection does not renew access.
Keep a provider scoped to one application identity and use separate clients for separate identities.
Changing the provider's returned token does not replace the identity of an already authenticated connection.

The SDK's connection warmup establishes TCP reachability. TLS and the authentication exchange are lazy and may run with the first cache operation.
`ConnectAsync` success alone therefore does not prove that credentials were accepted or that the server is ready to serve cache operations.

## Expiry and renewal

The server applies the configured JWT issuer, audience, signature and lifetime validation during connection authentication.
The existing two-minute clock-skew allowance still applies. Each RPC must remain inside the accepted not-before and expiry bounds.
Expiry does not cancel an operation that the server has already admitted.

When the connection's authorization expires, the server requests graceful connection closure.
The SDK establishes a new transport and calls the provider again. Supply a refreshed token from that invocation.
A request refused before admission for connection renewal can retry under the existing operation deadline and retry limits.
Already admitted requests can finish while the old connection drains.

Invalid credentials on a fresh connection are terminal authentication failures, rather than ordinary transient connection errors.
The SDK does not repeatedly acquire credentials or fail over merely to retry a rejected token.
Network/TLS failures and temporary server capacity refusals retain transport retry behavior.

The current data-plane authorization policy requires an authenticated identity. Connection authentication does not add command ACLs,
cache-name permissions, identity-provider introspection or individual-token revocation.

## Signing keys and authority metadata

Static signing-key configuration is loaded at host startup. Restarting a host changes its configured trust and closes its connections.
See [JWT signing keys](jwt-signing-keys.md) for symmetric cutover and OIDC rotation procedures.

With an OIDC authority, the discovery issuer and explicitly configured issuers form the framework's accepted issuer set.
An explicit `JwtIssuer` (or a custom host's `ValidIssuer` / `ValidIssuers`) adds trusted issuers; it does not restrict trust to only that issuer.
Signing keys must still satisfy the configured signature validation. A configured static signing key can also validate tokens from the current metadata issuer.

OIDC metadata is managed by the framework and can be cached or refreshed independently of a connection's token.
Changes to supported validation options or active metadata require a new connection authentication.
Custom hosts must publish replacement JWT options through the options monitor. Concurrent in-place mutation of validation options or configuration objects is unsupported.
Framework last-known-good configuration is not an immediate token-revocation service.
Connections accepted through historical signing configuration are bounded by the history that the server can verify;
unverifiable or expired historical trust fails closed and requires current credentials or usable authority metadata.

Custom principal and identity types must provide thread-safe virtual `Clone()` implementations that return independent objects of the same runtime type.
Principal clones must preserve identity order and types and share no identities with their source.
Custom clones are responsible for independently owning hidden mutable state and preserving their virtual authorization behavior.
Malformed returned clones are rejected during connection authentication or request authentication, without fallback to base types.
Exceptions thrown by custom clones remain host hook errors.

## Transport

The authenticated cache transport uses a versioned authentication exchange inside TLS before the first cache request.
The token is encrypted on the wire. Tokens must fit within the protocol's 8,192-byte UTF-8 limit.
The server bounds authentication input, waiting and concurrent work.

Use a direct SDK-to-server connection or a layer-4 proxy that passes the encrypted transport through unchanged.
Do not put bearer metadata on data RPCs or share one physical authenticated connection between different identities.

Ordinary HTTP health probes still use the primary HTTPS port without the cache authentication exchange.
Remote `/metrics` and `/health/ready/details` retain their separate HTTP bearer-token access rules; those rules do not authenticate cache RPCs.
Internode forwarding and replication retain their dedicated [mTLS transport](internode-mtls.md).

Custom hosts should treat connection authentication hooks as connection-time validation.
There is no application RPC path during the initial exchange, and validation hooks do not run for every subsequent RPC.
Do not rely on a per-request validation callback to implement changing command permissions or immediate revocation.
Authorization uses a fresh copy of the accepted connection principal for each operation.

## Related documentation

- [Configuration](../configuration.md)
- [Bootstrap failover](../bootstrap-client-failover.md)
- [Diagnostics and health](../diagnostics.md)
