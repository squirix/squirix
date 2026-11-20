using System;
using System.Net.Http;
using System.Net.Security;
using System.Security.Cryptography.X509Certificates;

namespace Squirix.Server.TestKit.Networking;

/// <summary>Configures HTTP clients used by in-process and loopback integration tests so they do not route through a system proxy.</summary>
public static class LoopbackHttp
{
    /// <summary>The keepalive ping delay of the loopback handler.</summary>
    public static readonly TimeSpan KeepAlivePingDelay = TimeSpan.FromSeconds(10);

    /// <summary>The keepalive ping timeout of the loopback handler.</summary>
    public static readonly TimeSpan KeepAlivePingTimeout = TimeSpan.FromSeconds(20);

    private static bool AllowUntrustedDevHttps => string.Equals(Environment.GetEnvironmentVariable("SQUIRIX_ALLOW_UNTRUSTED_DEV_HTTPS"), "1", StringComparison.Ordinal);

    /// <summary>
    /// Creates a <see cref="SocketsHttpHandler" /> that bypasses the system proxy for loopback HTTPS gRPC clients.
    /// On developer machines this expects a trusted ASP.NET Core HTTPS development certificate
    /// (<c language="csharp">dotnet dev-certs https --trust</c>). On Windows/macOS CI, interactive trust is unavailable;
    /// set <c language="csharp">SQUIRIX_ALLOW_UNTRUSTED_DEV_HTTPS=1</c> (see <c language="csharp">tools/ci/ensure-dev-https-cert.sh</c>).
    /// </summary>
    /// <returns>A handler suitable for loopback HTTPS gRPC clients.</returns>
    public static SocketsHttpHandler CreateHandler() => CreateHandler(false);

    /// <summary>Creates a handler for HTTPS requests to a host IP when the dev certificate is issued for <c language="csharp">localhost</c>.</summary>
    /// <returns>A loopback handler that tolerates certificate name mismatch.</returns>
    public static SocketsHttpHandler CreateHandlerAllowingCertNameMismatch() => CreateHandler(true);

    internal static SslClientAuthenticationOptions CreateTlsOptions(bool allowNameMismatch = false)
    {
        var options = new SslClientAuthenticationOptions();
        if (allowNameMismatch)
            options.RemoteCertificateValidationCallback = static (_, certificate, _, errors) => AcceptsAspNetCoreDevCertificate(certificate, errors, true);
        else if (AllowUntrustedDevHttps)
            options.RemoteCertificateValidationCallback = static (_, certificate, _, errors) => AcceptsAspNetCoreDevCertificate(certificate, errors, false);

        return options;
    }

    private static SocketsHttpHandler CreateHandler(bool allowNameMismatch) =>
        new()
        {
            UseProxy = false,
            EnableMultipleHttp2Connections = true,
            KeepAlivePingPolicy = HttpKeepAlivePingPolicy.Always,
            KeepAlivePingDelay = KeepAlivePingDelay,
            KeepAlivePingTimeout = KeepAlivePingTimeout,
            SslOptions = CreateTlsOptions(allowNameMismatch),
        };

    private static bool AcceptsAspNetCoreDevCertificate(X509Certificate? certificate, SslPolicyErrors errors, bool allowNameMismatch)
    {
        if (errors is SslPolicyErrors.None)
            return true;

        if (allowNameMismatch && errors is SslPolicyErrors.RemoteCertificateNameMismatch)
            return true;

        if (!AllowUntrustedDevHttps || certificate == null)
            return false;

        // CI cannot interactively trust the ASP.NET Core HTTPS development certificate on Windows/macOS.
        // Accept only that well-known localhost development certificate when the sole policy issues are
        // untrusted root and/or name mismatch (IP hosts vs CN=localhost).
        var tolerated = SslPolicyErrors.RemoteCertificateChainErrors;
        if (allowNameMismatch)
            tolerated |= SslPolicyErrors.RemoteCertificateNameMismatch;

        return (errors & ~tolerated) == SslPolicyErrors.None && IsAspNetCoreHttpsDevelopmentCertificate(certificate);
    }

    private static bool IsAspNetCoreHttpsDevelopmentCertificate(X509Certificate certificate) => certificate.Subject.Equals("CN=localhost", StringComparison.OrdinalIgnoreCase);
}
