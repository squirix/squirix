# Test host authentication

Use a per-node `TestNodeSecurityOptions` override for authentication tests. This keeps parallel tests isolated from
process-wide environment settings.

- For symmetric JWT, use `TestJwtHelper.CreateRandomCredentials()` and
  `TestNodeSecurityOptions.FromJwtCredentials(...)`. See [the gRPC JWT test](../../tests/squirix.server/squirix.server.integration-tests/Surfaces/AuthTests.cs).
- For OIDC, start `MockOidcAuthority` and pass `authority.ToSecurityOptions(audience)`. The mock authority serves
  discovery and JWKS on loopback. See [the OIDC integration test](../../tests/squirix.server/squirix.server.integration-tests/Security/OidcJwtAuthIntegrationTests.cs).

`MockOidcAuthority` and `TestNodeSecurityOptions` live in `Squirix.Server.TestKit.Hosting`. An explicit override
replaces the environment-based security settings for that node. When `Security` is omitted,
`NodeIntegrationTestBase` leaves environment lookup enabled, while `BlackBoxCluster` supplies an empty override
for unauthenticated smoke tests.

For deployment settings, see [External authentication](../configuration.md#external-authentication).
