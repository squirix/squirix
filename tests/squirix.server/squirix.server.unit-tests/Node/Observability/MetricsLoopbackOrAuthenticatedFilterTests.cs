using System.Net;
using System.Security.Claims;
using Microsoft.AspNetCore.Http;
using Squirix.Server.Node.Observability.Metrics;
using Xunit;

namespace Squirix.Server.UnitTests.Node.Observability;

/// <summary>
/// Unit tests for metrics endpoint loopback-or-authenticated access control.
/// </summary>
public sealed class MetricsLoopbackOrAuthenticatedFilterTests
{
    /// <summary>
    /// Verifies loopback clients can scrape metrics without authentication.
    /// </summary>
    [Fact]
    public void IsRequestAuthorizedAllowsLoopbackWithoutAuthentication()
    {
        var http = new DefaultHttpContext
        {
            Connection =
            {
                RemoteIpAddress = IPAddress.Loopback,
            },
        };

        Assert.True(SquirixMetricsConnectionSecurity.IsRequestAuthorized(http));
    }

    /// <summary>
    /// Verifies remote authenticated clients can scrape metrics.
    /// </summary>
    [Fact]
    public void IsRequestAuthorizedAllowsRemoteWhenAuthenticated()
    {
        var http = new DefaultHttpContext
        {
            User = new ClaimsPrincipal(new ClaimsIdentity([new Claim(ClaimTypes.Name, "scraper")], "Bearer")),
            Connection =
            {
                RemoteIpAddress = IPAddress.Parse("203.0.113.10"),
            },
        };

        Assert.True(SquirixMetricsConnectionSecurity.IsRequestAuthorized(http));
    }

    /// <summary>
    /// Verifies remote unauthenticated clients are rejected.
    /// </summary>
    [Fact]
    public void IsRequestAuthorizedRejectsRemoteWithoutAuthentication()
    {
        var http = new DefaultHttpContext
        {
            Connection =
            {
                RemoteIpAddress = IPAddress.Parse("203.0.113.10"),
            },
        };

        Assert.False(SquirixMetricsConnectionSecurity.IsRequestAuthorized(http));
    }
}
