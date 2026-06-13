using System.Net;
using Microsoft.AspNetCore.Http;
using Squirix.Server.Node.Observability.Metrics;
using Xunit;

namespace Squirix.Server.UnitTests.Node.Observability;

/// <summary>
/// Unit tests for metrics endpoint loopback client detection.
/// </summary>
public sealed class SquirixMetricsConnectionSecurityTests
{
    /// <summary>
    /// Verifies remote addresses are not treated as local clients.
    /// </summary>
    [Fact]
    public void IsLoopbackClientReturnsFalseForRemoteAddress()
    {
        var http = new DefaultHttpContext
        {
            Connection =
            {
                RemoteIpAddress = IPAddress.Parse("198.51.100.7"),
            },
        };

        Assert.False(SquirixMetricsConnectionSecurity.IsLoopbackClient(http));
    }

    /// <summary>
    /// Verifies missing remote addresses are not treated as local clients.
    /// </summary>
    [Fact]
    public void IsLoopbackClientReturnsFalseWhenRemoteAddressMissing()
    {
        var http = new DefaultHttpContext();

        Assert.False(SquirixMetricsConnectionSecurity.IsLoopbackClient(http));
    }

    /// <summary>
    /// Verifies IPv4 loopback addresses are treated as local clients.
    /// </summary>
    [Fact]
    public void IsLoopbackClientReturnsTrueForIpv4Loopback()
    {
        var http = new DefaultHttpContext
        {
            Connection =
            {
                RemoteIpAddress = IPAddress.Loopback,
            },
        };

        Assert.True(SquirixMetricsConnectionSecurity.IsLoopbackClient(http));
    }

    /// <summary>
    /// Verifies IPv6 loopback addresses are treated as local clients.
    /// </summary>
    [Fact]
    public void IsLoopbackClientReturnsTrueForIpv6Loopback()
    {
        var http = new DefaultHttpContext
        {
            Connection =
            {
                RemoteIpAddress = IPAddress.IPv6Loopback,
            },
        };

        Assert.True(SquirixMetricsConnectionSecurity.IsLoopbackClient(http));
    }
}
