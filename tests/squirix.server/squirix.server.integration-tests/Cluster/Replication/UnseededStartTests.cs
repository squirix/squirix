using System;
using System.IO;
using System.Net;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.IntegrationTests.Support;
using Squirix.Server.Storage;
using Squirix.Server.Storage.Replication;
using Squirix.Server.TestKit;
using Squirix.Server.TestKit.Hosting;
using Squirix.Server.TestKit.IO;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.IntegrationTests.Cluster.Replication;

/// <summary>A new cluster starts a group only once every member of it is up, and a node refuses a group metadata file of an earlier build.</summary>
public sealed class UnseededStartTests : NodeIntegrationTestBase
{
    private static readonly string[] Nodes = WipedCluster.Nodes;

    /// <summary>Two of three nodes up serve nothing and report not ready; the third node starts the groups, and then every node serves its own keys.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task PartialStartServesNothing(CancellationToken cancellationToken)
    {
        await using var cluster = CreateCluster([new ClusterNode(Nodes[0], GetNextHttpUri()), new ClusterNode(Nodes[1], GetNextHttpUri()), new ClusterNode(Nodes[2], GetNextHttpUri())]);
        var options = WipedCluster.Options("unseeded-partial-start", false);
        var first = await cluster.StartNodeAsync(Nodes[0], options, cancellationToken);
        var second = await cluster.StartNodeAsync(Nodes[1], options, cancellationToken);

        await WipedCluster.AssertUnseededAsync(first, cancellationToken);
        await WipedCluster.AssertUnseededAsync(second, cancellationToken);
        await WipedCluster.AssertRefusesWriteAsync(first, Nodes[0], cancellationToken);
        await WipedCluster.AssertRefusesWriteAsync(second, Nodes[1], cancellationToken);
        using (var response = await HttpClient.GetAsync(new Uri(first.Uri, "/health/ready"), cancellationToken))
            _ = await Assert.That(response.StatusCode).IsEqualTo(HttpStatusCode.ServiceUnavailable);

        _ = await cluster.StartNodeAsync(Nodes[2], options, cancellationToken);
        await WipedCluster.AwaitReadyAsync(cluster, Nodes, cancellationToken);

        foreach (var node in Nodes)
        {
            await ReplicaGroupFollowers.AwaitVerifiedAsync(cluster[node], cancellationToken);
            await WipedCluster.WriteAndReadAsync(cluster[node], node, cancellationToken);
        }
    }

    /// <summary>A node whose metadata of a group was written by an earlier build does not start, and its failure names the earlier build.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task EarlierBuildMetadataRefusesStart(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-unseeded-earlier-build");
        _ = Directory.CreateDirectory(GroupStoragePaths.GetGroupDirectory(dir, Nodes[0]));
        var earlier = new byte[GroupMetadataSlots.FileSize];
        Encoding.ASCII.GetBytes("SQRM").CopyTo(earlier, 0);
        await File.WriteAllBytesAsync(GroupStoragePaths.GetMetadataPath(dir, Nodes[0]), earlier, cancellationToken);
        await using var cluster = CreateCluster([new ClusterNode(Nodes[0], GetNextHttpUri()), new ClusterNode(Nodes[1], GetNextHttpUri()), new ClusterNode(Nodes[2], GetNextHttpUri())]);
        var options = WipedCluster.Options("unseeded-earlier-build", false, new PersistenceOptions { DataDir = dir });

        var failure = await NodeAsyncAssert.ThrowsAnyAsync<Exception, ITestNodeHost>(cluster.StartNodeAsync(Nodes[0], options, cancellationToken));

        _ = await Assert.That(failure.ToString()).Contains("Replica group 'node-a' metadata was written by an earlier build and is not supported", StringComparison.Ordinal);
        _ = await Assert.That(cluster.StartedCount).IsEqualTo(0);
    }
}
