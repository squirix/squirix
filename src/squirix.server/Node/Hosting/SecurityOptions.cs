using Squirix.Server.Attributes;

namespace Squirix.Server.Node.Hosting;

/// <summary>
/// Programmatic security configuration for in-process node hosts.
/// When supplied as an override, values replace environment-variable lookup for that node startup.
/// </summary>
[Immutable]
internal sealed record SecurityOptions
{
    /// <summary>Gets a value indicating whether non-HTTPS authority metadata is allowed (dev/test only).</summary>
    internal bool JwtAllowHttpMetadata { get; init; }

    /// <summary>Gets the JWT audience validation value. Required whenever JWT authentication is enabled.</summary>
    internal string? JwtAudience { get; init; }

    /// <summary>Gets the OIDC authority URL used for metadata discovery and JWKS validation.</summary>
    internal string? JwtAuthority { get; init; }

    /// <summary>Gets the JWT issuer. Required when using <see cref="JwtSigningKey" /> without an authority URL.</summary>
    internal string? JwtIssuer { get; init; }

    /// <summary>Gets the symmetric JWT signing key: base64 of at least 32 bytes.</summary>
    internal string? JwtSigningKey { get; init; }
}
