using System;
using System.Net;

namespace Squirix.Server.Node.Hosting;

/// <summary>
/// Enforces secure-by-default authentication for cache data-plane listeners bound on non-loopback interfaces.
/// </summary>
internal static class SquirixExternalAccessSecurity
{
    /// <summary>
    /// Refuses startup when the primary listen URL is non-loopback and API/JWT auth is not configured.
    /// </summary>
    /// <param name="listenUri">Primary node listen URI from cluster configuration.</param>
    /// <param name="authEnabled">Whether API key or JWT authentication was registered.</param>
    /// <exception cref="InvalidOperationException">Non-loopback listen without credentials.</exception>
    public static void EnsureDataPlaneAuthenticatedForListenUri(Uri listenUri, bool authEnabled)
    {
        ArgumentNullException.ThrowIfNull(listenUri);

        if (authEnabled || IsLoopbackHost(listenUri.Host))
            return;

        throw new InvalidOperationException(
            $"Refusing to start with unauthenticated cache endpoints on non-loopback interface ({listenUri}). " +
            "Configure SQUIRIX_API_KEYS and/or JWT settings.");
    }

    internal static bool IsLoopbackHost(string host) =>
        string.Equals(host, "localhost", StringComparison.OrdinalIgnoreCase) || (IPAddress.TryParse(host, out var ip) && IPAddress.IsLoopback(ip));
}
