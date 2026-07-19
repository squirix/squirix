using System;
using System.Net.Http;
using System.Net.Security;

namespace Squirix.Server.TestKit.Networking;

/// <summary>Configures HTTP clients used by in-process and loopback integration tests so they do not route through a system proxy.</summary>
public static class LoopbackHttp
{
    /// <summary>
    /// Creates a <see cref="SocketsHttpHandler" /> that bypasses the system proxy for loopback HTTPS gRPC clients.
    /// On developer machines this expects a trusted ASP.NET Core HTTPS development certificate
    /// (<c>dotnet dev-certs https --trust</c>). On Windows/macOS CI, interactive trust is unavailable;
    /// set <c>SQUIRIX_ALLOW_UNTRUSTED_DEV_HTTPS=1</c> (see <c>tools/ci/ensure-dev-https-cert.sh</c>).
    /// </summary>
    /// <returns>A handler suitable for loopback HTTPS gRPC clients.</returns>
    public static SocketsHttpHandler CreateHandler()
    {
        var handler = new SocketsHttpHandler
        {
            UseProxy = false,
            EnableMultipleHttp2Connections = true,
        };

        if (AllowUntrustedDevHttps)
            handler.SslOptions.RemoteCertificateValidationCallback = static (_, _, _, _) => true;

        return handler;
    }

    /// <summary>
    /// Creates a handler for HTTPS requests to a host IP when the dev certificate is issued for <c>localhost</c>.
    /// </summary>
    /// <returns>A loopback handler that tolerates certificate name mismatch.</returns>
    public static SocketsHttpHandler CreateHandlerAllowingCertificateNameMismatch()
    {
        var handler = CreateHandler();
        if (AllowUntrustedDevHttps)
        {
            // CreateHandler already accepts any certificate when CI opt-in is set.
            return handler;
        }

        handler.SslOptions.RemoteCertificateValidationCallback = static (_, _, _, errors) =>
            errors is SslPolicyErrors.None or SslPolicyErrors.RemoteCertificateNameMismatch;
        return handler;
    }

    private static bool AllowUntrustedDevHttps =>
        string.Equals(Environment.GetEnvironmentVariable("SQUIRIX_ALLOW_UNTRUSTED_DEV_HTTPS"), "1", StringComparison.Ordinal);
}
