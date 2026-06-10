using System.Collections.Generic;

namespace Squirix.Server.Node.Hosting;

/// <summary>
/// Programmatic security configuration for in-process node hosts.
/// When supplied as an override, values replace environment-variable lookup for that node startup.
/// </summary>
internal sealed record SecurityOptions
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
}
