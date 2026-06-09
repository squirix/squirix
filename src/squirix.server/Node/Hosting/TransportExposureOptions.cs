namespace Squirix.Server.Node.Hosting;

/// <summary>
/// Programmatic transport exposure settings for in-process node hosts.
/// When supplied as an override, values replace environment-variable lookup for that node startup.
/// </summary>
internal sealed record TransportExposureOptions
{
    /// <summary>
    /// Gets the plaintext HTTP/1 sidecar port. When <c>null</c>, no sidecar listener is configured.
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
}
