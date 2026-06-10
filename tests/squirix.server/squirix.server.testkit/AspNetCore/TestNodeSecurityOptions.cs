using System.Collections.Generic;
using Squirix.Server.Node.Hosting;

namespace Squirix.Server.TestKit.AspNetCore;

/// <summary>
/// Per-node security settings for in-process test hosts.
/// When provided to <see cref="TestNodeHostFactory" />, replaces process environment variables for that startup.
/// </summary>
public sealed class TestNodeSecurityOptions
{
    /// <summary>
    /// Gets API keys accepted by the node. When <c>null</c> or empty, API key auth is disabled.
    /// </summary>
    public IReadOnlyCollection<string>? ApiKeys { get; init; }

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

    internal SecurityOptions ToServerOptions() => new()
    {
        ApiKeys = ApiKeys,
        JwtAudience = JwtAudience,
        JwtIssuer = JwtIssuer,
        JwtSigningKey = JwtSigningKey,
    };
}
