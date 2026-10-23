using System;
using System.Diagnostics.CodeAnalysis;
using System.Net;

namespace Squirix.Server.Node.Hosting;

/// <summary>Enforces secure-by-default authentication for cache data-plane listeners bound on non-loopback interfaces.</summary>
internal static class ExternalAccessSecurity
{
    /// <summary>Refuses startup when the primary listen URI is non-loopback and JWT auth is not configured.</summary>
    /// <param name="listenUri">Primary node listen URI from cluster configuration.</param>
    /// <param name="authEnabled">Whether JWT authentication was registered.</param>
    /// <exception cref="InvalidOperationException">Non-loopback listen without credentials.</exception>
    internal static void EnsureDataPlaneAuthenticatedForListenUri(Uri listenUri, bool authEnabled)
    {
        ArgumentNullException.ThrowIfNull(listenUri);

        if (authEnabled || IsLoopbackHost(listenUri.Host))
            return;

        throw new InvalidOperationException(
            $"Refusing to start with unauthenticated cache endpoints on non-loopback interface ({listenUri}). " +
            "Configure JWT settings (SQUIRIX_JWT_SIGNING_KEY or SQUIRIX_JWT_AUTHORITY).");
    }

    internal static bool IsLoopbackHost(string host) =>
        string.Equals(host, "localhost", StringComparison.OrdinalIgnoreCase) || (IPAddress.TryParse(host, out var ip) && IPAddress.IsLoopback(ip));

    /// <summary>Resolves the exact loopback address when the host is a loopback IP literal.</summary>
    /// <param name="host">Host component of the listen URI.</param>
    /// <param name="address">The loopback address, or <see langword="null" /> when the host is not a loopback IP literal.</param>
    /// <returns><see langword="true" /> when the host is a loopback IP literal; <see langword="false" /> for names and non-loopback addresses.</returns>
    internal static bool TryGetLoopbackAddress(string host, [NotNullWhen(true)] out IPAddress? address)
    {
        if (IPAddress.TryParse(host, out var parsed) && IPAddress.IsLoopback(parsed))
        {
            address = parsed;
            return true;
        }

        address = null;
        return false;
    }
}
