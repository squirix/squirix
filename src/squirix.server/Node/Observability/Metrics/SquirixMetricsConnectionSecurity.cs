using System;
using System.Net;
using Microsoft.AspNetCore.Http;

namespace Squirix.Server.Node.Observability.Metrics;

/// <summary>
/// Connection-origin and authorization checks for the Prometheus metrics scrape endpoint.
/// </summary>
internal static class SquirixMetricsConnectionSecurity
{
    /// <summary>
    /// Returns <see langword="true" /> when the request may scrape metrics under the configured access mode.
    /// </summary>
    /// <param name="httpContext">The active HTTP context.</param>
    /// <param name="accessMode">Configured metrics access mode.</param>
    /// <returns><see langword="true" /> when the caller may scrape metrics.</returns>
    internal static bool IsRequestAuthorized(HttpContext httpContext, MetricsEndpointAccessMode accessMode)
    {
        ArgumentNullException.ThrowIfNull(httpContext);

        if (accessMode == MetricsEndpointAccessMode.Anonymous)
            return true;

        return IsLoopbackClient(httpContext) || httpContext.User.Identity?.IsAuthenticated == true;
    }

    /// <summary>
    /// Returns <see langword="true" /> when the request arrived over a loopback client connection.
    /// </summary>
    /// <param name="httpContext">The active HTTP context.</param>
    /// <returns><see langword="true" /> for localhost / loopback clients; otherwise <see langword="false" />.</returns>
    internal static bool IsLoopbackClient(HttpContext httpContext)
    {
        ArgumentNullException.ThrowIfNull(httpContext);

        var remote = httpContext.Connection.RemoteIpAddress;
        if (remote is null)
            return false;

        if (remote.IsIPv4MappedToIPv6)
            remote = remote.MapToIPv4();

        return IPAddress.IsLoopback(remote);
    }
}
