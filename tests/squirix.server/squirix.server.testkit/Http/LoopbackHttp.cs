using System;
using System.Diagnostics.CodeAnalysis;
using System.Net.Http;

namespace Squirix.Server.TestKit.Http;

/// <summary>
/// Configures HTTP clients used by in-process and loopback integration tests so they do not route through a system proxy.
/// </summary>
public static class LoopbackHttp
{
    /// <summary>
    /// Prevents local test traffic from using HTTP(S)_PROXY environment variables.
    /// </summary>
    public static void DisableSystemProxyForLocalTests()
    {
        const string noProxy = "localhost,127.0.0.1,::1";
        Environment.SetEnvironmentVariable("NO_PROXY", noProxy);
        Environment.SetEnvironmentVariable("no_proxy", noProxy);
    }

    /// <summary>
    /// Creates a <see cref="SocketsHttpHandler" /> that bypasses the system proxy.
    /// </summary>
    /// <param name="enableCleartextHttp2">When true, enables cleartext HTTP/2 for gRPC over <c>http://</c>.</param>
    /// <returns>A handler suitable for loopback test clients.</returns>
    public static SocketsHttpHandler CreateHandler(bool enableCleartextHttp2 = false)
    {
        if (enableCleartextHttp2)
            AppContext.SetSwitch("System.Net.Http.SocketsHttpHandler.Http2UnencryptedSupport", true);

        return new SocketsHttpHandler
        {
            UseProxy = false,
            EnableMultipleHttp2Connections = enableCleartextHttp2,
        };
    }

    /// <summary>
    /// Creates an <see cref="HttpClient" /> for loopback REST or sidecar probes.
    /// </summary>
    /// <param name="timeout">Optional request timeout; defaults to 30 seconds.</param>
    /// <returns>An HTTP/1.1 client that does not use the system proxy.</returns>
    [SuppressMessage("Reliability", "CA2000:Dispose objects before losing scope", Justification = "HttpClient owns the handler when disposeHandler is true.")]
    public static HttpClient CreateRestClient(TimeSpan? timeout = null)
    {
        var handler = CreateHandler();
        return new HttpClient(handler, true)
        {
            DefaultRequestVersion = System.Net.HttpVersion.Version11,
            DefaultVersionPolicy = HttpVersionPolicy.RequestVersionOrLower,
            Timeout = timeout ?? TimeSpan.FromSeconds(30),
        };
    }
}
