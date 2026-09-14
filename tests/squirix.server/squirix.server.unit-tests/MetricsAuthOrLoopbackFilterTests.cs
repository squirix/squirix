using System.Net;
using System.Security.Claims;
using Microsoft.AspNetCore.Http;
using Squirix.Server.Attributes;
using Squirix.Server.Node.Observability.Metrics;
using Xunit;

namespace Squirix.Server.UnitTests;

/// <summary>Unit tests for metrics endpoint loopback-or-authenticated access control.</summary>
[Immutable]
public sealed class MetricsAuthOrLoopbackFilterTests
{
    /// <summary>Verifies loopback clients can scrape metrics without authentication.</summary>
    [Fact]
    public void LoopbackAllowedWithoutAuthentication() => Assert.True(ConnectionSecurity.IsRequestAuthorized(CreateContext(IPAddress.Loopback)));

    /// <summary>Verifies remote authenticated clients can scrape metrics.</summary>
    [Fact]
    public void RemoteAllowedWhenAuthenticated() => Assert.True(ConnectionSecurity.IsRequestAuthorized(CreateContext(IPAddress.Parse("203.0.113.10"), true)));

    /// <summary>Verifies remote unauthenticated clients are rejected.</summary>
    [Fact]
    public void UnauthenticatedRemoteRejected() => Assert.False(ConnectionSecurity.IsRequestAuthorized(CreateContext(IPAddress.Parse("203.0.113.10"))));

    private static DefaultHttpContext CreateContext(IPAddress remoteIp, bool authenticated = false)
    {
        var http = new DefaultHttpContext
        {
            Connection =
            {
                RemoteIpAddress = remoteIp,
            },
        };

        if (authenticated)
            http.User = new ClaimsPrincipal(new ClaimsIdentity([new Claim(ClaimTypes.Name, "scraper")], "Bearer"));

        return http;
    }
}
