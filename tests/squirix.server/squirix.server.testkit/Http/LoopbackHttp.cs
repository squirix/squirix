using System;
using System.Diagnostics;
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
    /// Ensures loopback HTTPS clients can validate the ASP.NET Core development certificate.
    /// </summary>
    public static void EnsureDevelopmentCertificateTrusted()
    {
        if (IsDevelopmentCertificateTrusted())
            return;

        var exitCode = RunDotnet(["dev-certs", "https", "--trust"]);
        if (exitCode != 0 || !IsDevelopmentCertificateTrusted())
        {
            throw new InvalidOperationException(
                "The ASP.NET Core HTTPS development certificate is not trusted. Run: dotnet dev-certs https --trust");
        }
    }

    /// <summary>
    /// Creates a <see cref="SocketsHttpHandler" /> that bypasses the system proxy for loopback HTTPS gRPC clients.
    /// Requires the ASP.NET Core development certificate to be trusted. See <see cref="EnsureDevelopmentCertificateTrusted" />.
    /// </summary>
    /// <returns>A handler suitable for loopback HTTPS gRPC clients.</returns>
    public static SocketsHttpHandler CreateHandler() => new()
    {
        UseProxy = false,
        EnableMultipleHttp2Connections = true,
    };

    /// <summary>
    /// Creates an <see cref="HttpClient" /> for loopback REST or sidecar probes.
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
            DefaultRequestVersion = System.Net.HttpVersion.Version11,
            DefaultVersionPolicy = HttpVersionPolicy.RequestVersionOrLower,
            Timeout = timeout ?? TimeSpan.FromSeconds(30),
        };
    }

    private static bool IsDevelopmentCertificateTrusted() =>
        RunDotnet(["dev-certs", "https", "--check", "--trust"]) == 0;

    private static int RunDotnet(string[] args)
    {
        using var process = Process.Start(new ProcessStartInfo
        {
            FileName = "dotnet",
            Arguments = string.Join(' ', args),
            UseShellExecute = false,
        });

        process?.WaitForExit();
        return process?.ExitCode ?? 1;
    }
}
