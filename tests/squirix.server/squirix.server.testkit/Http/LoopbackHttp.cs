using System.Net.Http;
using System.Net.Security;

namespace Squirix.Server.TestKit.Http;

/// <summary>
/// Configures HTTP clients used by in-process and loopback integration tests so they do not route through a system proxy.
/// </summary>
public static class LoopbackHttp
{
    /// <summary>
    /// Creates a <see cref="SocketsHttpHandler" /> that bypasses the system proxy for loopback HTTPS gRPC clients.
    /// Requires a trusted ASP.NET Core HTTPS development certificate on the machine running tests
    /// (<c>dotnet dev-certs https --trust</c>).
    /// </summary>
    /// <returns>A handler suitable for loopback HTTPS gRPC clients.</returns>
    public static SocketsHttpHandler CreateHandler() => new()
    {
        UseProxy = false,
        EnableMultipleHttp2Connections = true,
    };

    /// <summary>
    /// Creates a handler for HTTPS requests to a host IP when the dev certificate is issued for <c>localhost</c>.
    /// </summary>
    /// <returns>A loopback handler that tolerates certificate name mismatch.</returns>
    public static SocketsHttpHandler CreateHandlerAllowingCertificateNameMismatch()
    {
        var handler = CreateHandler();
        handler.SslOptions.RemoteCertificateValidationCallback = static (_, _, _, errors) =>
            errors is SslPolicyErrors.None or SslPolicyErrors.RemoteCertificateNameMismatch;
        return handler;
    }
}
