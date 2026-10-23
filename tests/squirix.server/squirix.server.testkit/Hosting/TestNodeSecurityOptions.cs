using System;
using Squirix.Server.Attributes;
using Squirix.Server.Node.Hosting;

namespace Squirix.Server.TestKit.Hosting;

/// <summary>
/// Per-node security settings for in-process test hosts.
/// When provided to a test node start, replaces process environment variables for that startup.
/// </summary>
[Immutable]
public sealed class TestNodeSecurityOptions
{
    /// <summary>Gets a value indicating whether non-HTTPS authority metadata is allowed (dev/test only).</summary>
    public bool JwtAllowHttpMetadata { get; init; }

    /// <summary>Gets the JWT audience validation value.</summary>
    public string? JwtAudience { get; init; }

    /// <summary>Gets the OIDC authority URL used for metadata discovery and JWKS validation.</summary>
    public string? JwtAuthority { get; init; }

    /// <summary>Gets the JWT issuer. Required when using <see cref="JwtSigningKey" /> without an authority URL.</summary>
    public string? JwtIssuer { get; init; }

    /// <summary>Gets the symmetric JWT signing key, raw text or base64.</summary>
    public string? JwtSigningKey { get; init; }

    /// <summary>Maps symmetric JWT credentials to node security options.</summary>
    /// <param name="credentials">Symmetric JWT credentials.</param>
    /// <returns>Per-node security override for in-process test hosts.</returns>
    public static TestNodeSecurityOptions FromJwtCredentials(TestJwtCredentials credentials)
    {
        ArgumentNullException.ThrowIfNull(credentials);

        return new TestNodeSecurityOptions
        {
            JwtSigningKey = credentials.Base64SigningKey,
            JwtIssuer = credentials.Issuer,
            JwtAudience = credentials.Audience,
        };
    }

    internal SecurityOptions ToServerOptions() => new()
    {
        JwtAudience = JwtAudience,
        JwtIssuer = JwtIssuer,
        JwtSigningKey = JwtSigningKey,
        JwtAuthority = JwtAuthority,
        JwtAllowHttpMetadata = JwtAllowHttpMetadata,
    };
}
