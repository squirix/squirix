using System;
using System.Net;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Authentication;
using Microsoft.AspNetCore.Authentication.JwtBearer;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.DependencyInjection;

namespace Squirix.Server.Node.Observability.Metrics;

/// <summary>Connection-origin and authorization checks for the Prometheus metrics scrape endpoint.</summary>
internal static class ConnectionSecurity
{
    /// <summary>
    /// Returns <see langword="true" /> when the request may scrape metrics.
    /// Loopback clients are allowed without credentials; all other clients must authenticate with the bearer scheme.
    /// </summary>
    /// <param name="httpContext">The active HTTP context.</param>
    /// <returns><see langword="true" /> when the caller may scrape metrics.</returns>
    internal static async ValueTask<bool> IsRequestAuthorizedAsync(HttpContext httpContext)
    {
        ArgumentNullException.ThrowIfNull(httpContext);
        if (IsLoopbackClient(httpContext))
            return true;

        var schemes = httpContext.RequestServices.GetService<IAuthenticationSchemeProvider>();
        if (schemes == null || await schemes.GetSchemeAsync(JwtBearerDefaults.AuthenticationScheme).ConfigureAwait(false) == null)
            return false;

        var result = await httpContext.AuthenticateAsync(JwtBearerDefaults.AuthenticationScheme).ConfigureAwait(false);
        return result.Succeeded;
    }

    /// <summary>Returns <see langword="true" /> when the request arrived over a loopback client connection.</summary>
    /// <param name="httpContext">The active HTTP context.</param>
    /// <returns><see langword="true" /> for localhost / loopback clients; otherwise <see langword="false" />.</returns>
    private static bool IsLoopbackClient(HttpContext httpContext)
    {
        ArgumentNullException.ThrowIfNull(httpContext);

        var remote = httpContext.Connection.RemoteIpAddress;
        if (remote == null)
            return false;

        if (remote.IsIPv4MappedToIPv6)
            remote = remote.MapToIPv4();

        return IPAddress.IsLoopback(remote);
    }
}
