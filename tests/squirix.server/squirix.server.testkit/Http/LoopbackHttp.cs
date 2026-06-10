using System;
using System.Diagnostics.CodeAnalysis;
using System.Net;
using System.Net.Http;

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
    /// Creates an <see cref="HttpClient" /> for loopback REST probes over HTTPS.
    /// Requires a trusted ASP.NET Core HTTPS development certificate on the machine running tests
    /// (<c>dotnet dev-certs https --trust</c>).
    /// </summary>
    /// <param name="timeout">Optional request timeout; defaults to 30 seconds.</param>
    /// <returns>An HTTP/1.1 client that does not use the system proxy.</returns>
    [SuppressMessage("Reliability", "CA2000:Dispose objects before losing scope", Justification = "HttpClient owns the handler when disposeHandler is true.")]
    public static HttpClient CreateRestClient(TimeSpan? timeout = null)
    {
        var handler = new SocketsHttpHandler
        {
            UseProxy = false,
        };
        return new HttpClient(handler, true)
        {
            DefaultRequestVersion = HttpVersion.Version11,
            DefaultVersionPolicy = HttpVersionPolicy.RequestVersionOrLower,
            Timeout = timeout ?? TimeSpan.FromSeconds(30),
        };
    }
}
