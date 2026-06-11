using System;
using Squirix.Server.Cluster.Membership;
using Squirix.Server.Node.Hosting;
using Xunit;

namespace Squirix.Server.UnitTests.Node.Hosting;

/// <summary>
/// Unit tests for Kestrel HTTPS listener configuration.
/// </summary>
public sealed class SquirixKestrelConfigurationTests
{
    /// <summary>
    /// Ensures plaintext HTTP cluster URLs are rejected.
    /// </summary>
    [Fact]
    public void EnsureHttpsTransportRejectsPlaintextHttpUrl()
    {
        var cluster = new ClusterConfig
        {
            ClusterId = "test",
            NodeId = "node-a",
            Url = "http://localhost:5001",
        };

        var ex = Assert.Throws<InvalidOperationException>(() => SquirixKestrelConfiguration.EnsureHttpsTransport(cluster));
        Assert.Contains("HTTPS", ex.Message, StringComparison.Ordinal);
    }
}
