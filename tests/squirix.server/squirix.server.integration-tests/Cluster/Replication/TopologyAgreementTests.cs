using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Cluster.Replication;
using Squirix.Server.Core;
using Squirix.Server.IntegrationTests.Support;
using Squirix.Server.Storage.Journaling.Abstractions;
using Squirix.Server.Storage.Journaling.Read;
using Squirix.Server.Storage.Replication;
using Squirix.Server.TestKit;
using Squirix.Server.TestKit.Hosting;
using Squirix.Server.TestKit.IO;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.IntegrationTests.Cluster.Replication;

/// <summary>Activated topology agreement across restarts: matching identity stays ready, changes are refused.</summary>
public sealed class TopologyAgreementTests : NodeIntegrationTestBase
{
    private const string Unsupported = "Changing the activated topology of an existing data directory is not supported in this release";

    /// <summary>A restart with a new generation is refused at startup and names the changed generation.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task GenerationChangeIsRejected(CancellationToken cancellationToken)
    {
        var uriA = GetNextHttpUri();
        var uriB = GetNextHttpUri();
        var options = new IntegrationStartOptions { ReplicaCount = 2, UsePersistence = true, ExtraScope = "topology-generation" };

        await using var cluster = await StartClusterAsync([new ClusterNode("n1", uriA), new ClusterNode("n2", uriB)], options, cancellationToken);
        await cluster.StopNodeAsync("n1");

        var opt = new IntegrationStartOptions { ReplicaCount = 2, UsePersistence = true, CleanTestDir = false, ExtraScope = "topology-generation", ConfigurationGeneration = 2 };
        var exception = await NodeAsyncAssert.ThrowsAsync<InvalidOperationException, ITestNodeHost>(cluster.StartNodeAsync("n1", opt, cancellationToken));

        _ = await Assert.That(exception.Message).Contains(": generation changed (stamped 1, configured 2). ", StringComparison.Ordinal);
        _ = await Assert.That(exception.Message).Contains(Unsupported, StringComparison.Ordinal);
    }

    /// <summary>A refused restart surfaces the topology error and leaves a torn journal segment untouched, because the stamp is checked before the journal opens.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task RefusedRestartLeavesTornSegmentUntouched(CancellationToken cancellationToken)
    {
        var options = new IntegrationStartOptions { ReplicaCount = 2, UsePersistence = true, ExtraScope = "topology-torn-segment" };

        await using var cluster = await StartClusterAsync([new ClusterNode("n1", GetNextHttpUri()), new ClusterNode("n2", GetNextHttpUri())], options, cancellationToken);
        var dataDir = cluster["n1"].DataDir;
        await cluster.StopNodeAsync("n1");
        await JournalSegmentLeaseWait.WaitForReleasedAsync(dataDir, cancellationToken);

        var active = NodePathKit.Combine(dataDir, $"{FilePrefixes.Journal}{NodeInvariantIndexStrings.FormatD6(1)}{FileExtensions.Journal}");
        WriteTornSegment(active);
        var torn = await File.ReadAllBytesAsync(active, cancellationToken);

        var opt = new IntegrationStartOptions { ReplicaCount = 2, UsePersistence = true, CleanTestDir = false, ExtraScope = "topology-torn-segment", ConfigurationGeneration = 2 };
        var exception = await NodeAsyncAssert.ThrowsAsync<InvalidOperationException, ITestNodeHost>(cluster.StartNodeAsync("n1", opt, cancellationToken));

        _ = await Assert.That(exception.Message).Contains(": generation changed (stamped 1, configured 2). ", StringComparison.Ordinal);
        var after = await File.ReadAllBytesAsync(active, cancellationToken);
        _ = await Assert.That(after.AsSpan().SequenceEqual(torn)).IsTrue();
    }

    /// <summary>A restart with a different peer set but the same generation and replica count names the fingerprint change.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task PeerChangeIsRejected(CancellationToken cancellationToken)
    {
        var uriA = GetNextHttpUri();
        var uriB = GetNextHttpUri();
        var options = new IntegrationStartOptions { ReplicaCount = 2, UsePersistence = true, ExtraScope = "topology-peers" };

        await using var cluster = await StartClusterAsync([new ClusterNode("n1", uriA), new ClusterNode("n2", uriB)], options, cancellationToken);
        await cluster.StopNodeAsync("n1");

        // Same listen URIs with a renamed second peer: only the remaining fingerprint inputs change.
        ClusterNode[] changed = [new("n1", uriA), new("n3", uriB)];
        var opt = new IntegrationStartOptions { ReplicaCount = 2, UsePersistence = true, CleanTestDir = false, ExtraScope = "topology-peers" };
        var exception = await NodeAsyncAssert.ThrowsAsync<InvalidOperationException, ITestNodeHost>(
            cluster.StartNodeAsync(new ClusterNode("n1", uriA), changed, opt, cancellationToken));

        _ = await Assert.That(exception.Message).Contains(": topology fingerprint changed (stamped ", StringComparison.Ordinal);
        _ = await Assert.That(exception.Message).Contains(") while generation and replica count match, so the cluster id, virtual nodes, peers", StringComparison.Ordinal);
        _ = await Assert.That(exception.Message).DoesNotContain("generation changed", StringComparison.Ordinal);
        _ = await Assert.That(exception.Message).Contains(Unsupported, StringComparison.Ordinal);
    }

    /// <summary>A restart with a new replica count is refused at startup and names the changed replica count.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task ReplicaCountChangeIsRejected(CancellationToken cancellationToken)
    {
        var options = new IntegrationStartOptions { ReplicaCount = 2, UsePersistence = true, ExtraScope = "topology-replicas" };

        await using var cluster = await StartClusterAsync("n1", "n2", "n3", options, cancellationToken);
        await cluster.StopNodeAsync("n1");

        var opt = new IntegrationStartOptions { ReplicaCount = 3, UsePersistence = true, CleanTestDir = false, ExtraScope = "topology-replicas" };
        var exception = await NodeAsyncAssert.ThrowsAsync<InvalidOperationException, ITestNodeHost>(cluster.StartNodeAsync("n1", opt, cancellationToken));

        _ = await Assert.That(exception.Message).Contains(": replica count changed (stamped 2, configured 3). ", StringComparison.Ordinal);
        _ = await Assert.That(exception.Message).Contains(Unsupported, StringComparison.Ordinal);
    }

    /// <summary>A restart with the same activated identity starts and its group logs stay ready.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task RestartMatchingAllowsReadiness(CancellationToken cancellationToken)
    {
        var uriA = GetNextHttpUri();
        var uriB = GetNextHttpUri();
        var options = new IntegrationStartOptions { ReplicaCount = 2, UsePersistence = true, ExtraScope = "topology-restart" };

        await using var cluster = await StartClusterAsync([new ClusterNode("n1", uriA), new ClusterNode("n2", uriB)], options, cancellationToken);
        await cluster.StopNodeAsync("n1");

        var restarted = await cluster.StartNodeAsync(
            "n1",
            new IntegrationStartOptions { ReplicaCount = 2, UsePersistence = true, CleanTestDir = false, ExtraScope = "topology-restart" },
            cancellationToken);
        var registry = restarted.GetRequiredService<ReplicaGroupRegistry>();

        _ = await Assert.That(registry.TryGetLog("n1", out var log)).IsTrue();
        var status = await log!.GetStatusAsync(cancellationToken);
        _ = await Assert.That(status.Readiness).IsEqualTo(FollowerLogReadiness.Ready);
    }

    /// <summary>An RF=1 data directory with journal state is refused as RF=2 because that migration is unsupported.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task RfOneDataRejectedAsRfTwo(CancellationToken cancellationToken)
    {
        var options = new IntegrationStartOptions { UsePersistence = true, ExtraScope = "topology-rf1-data" };
        await using (var single = await StartClusterAsync("n1", options, cancellationToken))
        {
            var entry = new NodeCacheEntry<object?> { Value = "v" };
            await GetCache(single["n1"]).SetEntryAsync(Guid.NewGuid().ToString("N"), ServerCacheNames.DefaultNamespace, "rf1-key", entry, cancellationToken);
        }

        await using var cluster = CreateCluster([new ClusterNode("n1", GetNextHttpUri()), new ClusterNode("n2", GetNextHttpUri())]);
        var opt = new IntegrationStartOptions { ReplicaCount = 2, UsePersistence = true, CleanTestDir = false, ExtraScope = "topology-rf1-data" };
        var exception = await NodeAsyncAssert.ThrowsAsync<InvalidOperationException, ITestNodeHost>(cluster.StartNodeAsync("n1", opt, cancellationToken));

        _ = await Assert.That(exception.Message).StartsWith(
            "Data directory holds durable cache journal state but no activated topology stamp, so it was last used by an RF=1 node; " +
            "moving existing RF=1 data to RF>1 is not supported in this release.",
            StringComparison.Ordinal);
        _ = await Assert.That(exception.Message).EndsWith(
            "Start the RF>1 node on an empty data directory, or migrate the data at the application level.",
            StringComparison.Ordinal);
    }

    /// <summary>A data directory activated as RF=2 is refused as RF=1, whose startup would ignore the replica group logs.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task RfTwoDataRejectedAsRfOne(CancellationToken cancellationToken)
    {
        var options = new IntegrationStartOptions { ReplicaCount = 2, UsePersistence = true, ExtraScope = "topology-rf2-data" };

        await using var cluster = await StartClusterAsync([new ClusterNode("n1", GetNextHttpUri()), new ClusterNode("n2", GetNextHttpUri())], options, cancellationToken);
        await cluster.StopNodeAsync("n1");

        var opt = new IntegrationStartOptions { UsePersistence = true, CleanTestDir = false, ExtraScope = "topology-rf2-data" };
        var exception = await NodeAsyncAssert.ThrowsAsync<InvalidOperationException, ITestNodeHost>(cluster.StartNodeAsync("n1", opt, cancellationToken));

        _ = await Assert.That(exception.Message).IsEqualTo(
            "Data directory was activated for replica count 2 (generation 1); starting it as RF=1 is not supported in this release. " +
            "Start the node with the replica count the directory was activated with, or on an empty data directory.");
    }

    private static void WriteTornSegment(string path)
    {
        Span<byte> segment = stackalloc byte[JournalFraming.FileHeaderSize + 3];
        JournalFraming.WriteFileHeader(segment[..JournalFraming.FileHeaderSize]);
        segment[JournalFraming.FileHeaderSize] = 0x01;
        segment[JournalFraming.FileHeaderSize + 1] = 0x02;
        segment[JournalFraming.FileHeaderSize + 2] = 0x03;
        File.WriteAllBytes(path, segment);
    }
}
