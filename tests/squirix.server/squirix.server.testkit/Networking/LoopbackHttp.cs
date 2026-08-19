using System;
using System.Net.Http;
using System.Net.Security;
using System.Security.Cryptography.X509Certificates;

namespace Squirix.Server.TestKit.Networking;

/// <summary>Configures HTTP clients used by in-process and loopback integration tests so they do not route through a system proxy.</summary>
public static class LoopbackHttp
{
    private static bool AllowUntrustedDevHttps => string.Equals(Environment.GetEnvironmentVariable("SQUIRIX_ALLOW_UNTRUSTED_DEV_HTTPS"), "1", StringComparison.Ordinal);

    /// <summary>
    /// Creates a <see cref="SocketsHttpHandler" /> that bypasses the system proxy for loopback HTTPS gRPC clients.
    /// On developer machines this expects a trusted ASP.NET Core HTTPS development certificate
    /// (<c>dotnet dev-certs https --trust</c>). On macOS CI interactive trust is unavailable;
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
            handler.SslOptions.RemoteCertificateValidationCallback = static (_, certificate, _, errors) => AcceptsAspNetCoreHttpsDevelopmentCertificate(certificate, errors, false);

        return handler;
    }

    /// <summary>
    /// Creates a handler for HTTPS requests to a host IP when the dev certificate is issued for <c>localhost</c>.
    /// </summary>
    /// <returns>A loopback handler that tolerates certificate name mismatch.</returns>
    public static SocketsHttpHandler CreateHandlerAllowingCertificateNameMismatch()
    {
        var handler = CreateHandler();
        handler.SslOptions.RemoteCertificateValidationCallback = static (_, certificate, _, errors) => AcceptsAspNetCoreHttpsDevelopmentCertificate(certificate, errors, true);
        return handler;
    }

    private static bool AcceptsAspNetCoreHttpsDevelopmentCertificate(X509Certificate? certificate, SslPolicyErrors errors, bool allowNameMismatch)
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

        if ((errors & ~tolerated) != SslPolicyErrors.None)
            return false;

        return IsAspNetCoreHttpsDevelopmentCertificate(certificate);
    }

    private static bool IsAspNetCoreHttpsDevelopmentCertificate(X509Certificate certificate) => certificate.Subject.Equals("CN=localhost", StringComparison.OrdinalIgnoreCase);
}
