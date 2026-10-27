using System.Net;
using System.Security.Claims;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Http;
using Squirix.Server.Attributes;
using Squirix.Server.Node.Observability.Metrics;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests;

/// <summary>Unit tests for metrics endpoint loopback-or-authenticated access control.</summary>
[Immutable]
public sealed class MetricsAuthOrLoopbackFilterTests
{
    /// <summary>Verifies loopback clients can scrape metrics without authentication.</summary>
    [Test]
    public async Task LoopbackAllowedWithoutAuthentication() => _ = await Assert.That(ConnectionSecurity.IsRequestAuthorized(CreateContext(IPAddress.Loopback))).IsTrue();

    /// <summary>Verifies remote authenticated clients can scrape metrics.</summary>
    [Test]
    public async Task RemoteAllowedWhenAuthenticated() =>
        _ = await Assert.That(ConnectionSecurity.IsRequestAuthorized(CreateContext(IPAddress.Parse("203.0.113.10"), true))).IsTrue();

    /// <summary>Verifies remote unauthenticated clients are rejected.</summary>
    [Test]
    public async Task UnauthenticatedRemoteRejected() => _ = await Assert.That(ConnectionSecurity.IsRequestAuthorized(CreateContext(IPAddress.Parse("203.0.113.10")))).IsFalse();

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
