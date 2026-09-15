using System;
using System.Threading.Tasks;
using Squirix.Attributes;
using Squirix.Internal.Cluster.Reliability;
using Squirix.Internal.Cluster.Transport;
using Squirix.TestKit;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.UnitTests;

/// <summary>Regression coverage for <see cref="ClientPool" /> gRPC channel reuse (issue #1).</summary>
[Immutable]
public sealed class ClientPoolChannelReuseTests
{
    private const int LoopIterationCount = 256;
    private static readonly string[] ExpectedNodes = ["node-a", "node-b"];

    /// <summary>Repeated lookups for the same node must return the same gRPC client instance.</summary>
    [Test]
    public async Task ForNodeReusesClientAcrossLookupsAsync()
    {
        var peers = new[]
        {
            new Peer
            {
                NodeId = "node-a",
                Uri = new Uri("https://127.0.0.1:6500"),
            },
        };

        await using var pool = new ClientPool(peers, static _ => new CallPolicy());
        var first = pool.ForNode("node-a");

        for (var i = 0; i < LoopIterationCount; i++)
            _ = await Assert.That(pool.ForNode("node-a")).IsSameReferenceAs(first);
    }

    /// <summary>Many ForNode lookups must not grow the pooled channel count beyond the configured peer set.</summary>
    [Test]
    public async Task PoolSizeStableAfterManyLookupsAsync()
    {
        var peers = new[]
        {
            new Peer { NodeId = "node-a", Uri = new Uri("https://127.0.0.1:6501") },
            new Peer { NodeId = "node-b", Uri = new Uri("https://127.0.0.1:6502") },
        };

        await using var pool = new ClientPool(peers, static _ => new CallPolicy());
        await SequenceAssert.Equal(ExpectedNodes, pool.BootstrapNodeIds, StringComparer.Ordinal);

        var anchor = pool.ForNode("node-a");

        for (var i = 0; i < LoopIterationCount; i++)
            _ = pool.ForNode(i % 2 == 0 ? "node-a" : "node-b");

        _ = await Assert.That(pool.ForNode("node-a")).IsSameReferenceAs(anchor);
    }
}
