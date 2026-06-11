using Squirix.Server.Node.Hosting;

namespace Squirix.Server.TestKit.AspNetCore;

/// <summary>
/// Per-node security settings for in-process test hosts.
/// When provided to <see cref="TestNodeHostFactory" />, replaces process environment variables for that startup.
/// </summary>
public sealed class TestNodeSecurityOptions
{
    /// <summary>
    /// Gets the JWT audience validation value.
    /// </summary>
    public string? JwtAudience { get; init; }

    /// <summary>
    /// Gets the JWT issuer. Required when using <see cref="JwtSigningKey" /> without an authority URL.
    /// </summary>
    public string? JwtIssuer { get; init; }

    /// <summary>
    /// Gets the symmetric JWT signing key, raw text or base64.
    /// </summary>
    public string? JwtSigningKey { get; init; }

    /// <summary>
    /// Gets the OIDC authority URL used for metadata discovery and JWKS validation.
    /// </summary>
    public string? JwtAuthority { get; init; }

    /// <summary>
    /// Gets a value indicating whether non-HTTPS authority metadata is allowed (dev/test only).
    /// </summary>
    public bool JwtAllowHttpMetadata { get; init; }

    internal SecurityOptions ToServerOptions() => new()
    {
        JwtAudience = JwtAudience,
        JwtIssuer = JwtIssuer,
        JwtSigningKey = JwtSigningKey,
        JwtAuthority = JwtAuthority,
        JwtAllowHttpMetadata = JwtAllowHttpMetadata,
    };
}
