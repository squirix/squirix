using Squirix.Server.Node.Hosting;

namespace Squirix.Server.TestKit.AspNetCore;

/// <summary>
/// Per-node transport exposure settings for in-process test hosts.
/// When provided, replaces process environment variables for that startup.
/// </summary>
public sealed class TestNodeTransportExposureOptions
{
    /// <summary>
    /// Gets the plaintext HTTP/1 sidecar port. When unset, no sidecar listener is configured.
    /// </summary>
    public int? Http1SidecarPort { get; init; }

    /// <summary>
    /// Gets a value indicating whether unauthenticated non-loopback cache endpoints are allowed.
    /// </summary>
    public bool AllowUnauthenticatedExternal { get; init; }

    /// <summary>
    /// Gets a value indicating whether a non-loopback plaintext HTTP/1 sidecar is allowed.
    /// </summary>
    public bool AllowInsecureHttp1SidecarExternal { get; init; }

    internal TransportExposureOptions ToServerOptions() => new()
    {
        Http1SidecarPort = Http1SidecarPort,
        AllowUnauthenticatedExternal = AllowUnauthenticatedExternal,
        AllowInsecureHttp1SidecarExternal = AllowInsecureHttp1SidecarExternal,
    };
}
