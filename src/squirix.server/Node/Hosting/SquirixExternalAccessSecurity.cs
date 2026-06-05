using System;
using System.Net;
using Squirix.Server.Utils;

namespace Squirix.Server.Node.Hosting;

/// <summary>
/// Enforces secure-by-default authentication for cache data-plane listeners bound on non-loopback interfaces.
/// </summary>
internal static class SquirixExternalAccessSecurity
{
    private const string AllowUnauthenticatedExternalVariable = "SQUIRIX_ALLOW_UNAUTHENTICATED_EXTERNAL";

    /// <summary>
    /// Refuses startup when the primary listen URL is non-loopback and neither API/JWT auth nor an explicit insecure override is configured.
    /// </summary>
    /// <param name="listenUri">Primary node listen URI from cluster configuration.</param>
    /// <param name="authEnabled">Whether API key or JWT authentication was registered.</param>
    /// <param name="environmentName">Host environment name (for warning text).</param>
    /// <exception cref="InvalidOperationException">Non-loopback listen without credentials or explicit override.</exception>
    public static void EnsureDataPlaneAuthenticatedForListenUri(Uri listenUri, bool authEnabled, string environmentName)
    {
        ArgumentNullException.ThrowIfNull(listenUri);

        if (authEnabled || IsLoopbackHost(listenUri.Host))
            return;

        if (EnvVariables.ReadBool(AllowUnauthenticatedExternalVariable))
        {
            if (!string.Equals(environmentName, "Development", StringComparison.OrdinalIgnoreCase))
            {
                Console.Error.WriteLine(
                    $"WARNING: cache REST and gRPC endpoints are exposed without authentication on non-loopback interface ({listenUri}). " +
                    $"Set {AllowUnauthenticatedExternalVariable}=true only for explicitly insecure scenarios, or configure SQUIRIX_API_KEYS / JWT.");
            }

            return;
        }

        throw new InvalidOperationException(
            $"Refusing to start with unauthenticated cache endpoints on non-loopback interface ({listenUri}). " +
            $"Configure SQUIRIX_API_KEYS and/or JWT settings, or set {AllowUnauthenticatedExternalVariable}=true only for explicitly insecure scenarios.");
    }

    internal static bool IsLoopbackHost(string host) =>
        string.Equals(host, "localhost", StringComparison.OrdinalIgnoreCase) || (IPAddress.TryParse(host, out var ip) && IPAddress.IsLoopback(ip));
}
