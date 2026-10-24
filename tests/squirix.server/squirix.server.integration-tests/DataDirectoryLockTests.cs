using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Core;
using Squirix.Server.IntegrationTests.Support;
using Squirix.Server.Storage;
using Squirix.Server.TestKit;
using Squirix.Server.TestKit.Hosting;
using Squirix.Server.TestKit.IO;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.IntegrationTests;

/// <summary>A persistence data directory belongs to one running node at a time.</summary>
public sealed class DataDirectoryLockTests : NodeIntegrationTestBase
{
    private const string CacheName = "default";
    private const string RefusedScope = "data-dir-lock-refused";
    private const string ReplicatedScope = "data-dir-lock-replicated";
    private const string FirstScope = "data-dir-lock-first";
    private const string SecondScope = "data-dir-lock-second";

    /// <summary>A second host on the data directory of a running host fails to start while the first keeps serving, and the directory is free after the first stops.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task SecondHostOnHeldDirectoryFails(CancellationToken cancellationToken)
    {
        await using var first = await StartClusterAsync("node-a", new IntegrationStartOptions { UsePersistence = true, ExtraScope = FirstScope }, cancellationToken);
        var dataDir = first["node-a"].DataDir;
        var shared = new PersistenceOptions { DataDir = dataDir, JournalMaxSegmentMb = 64 };
        var secondOptions = new IntegrationStartOptions { PersistenceOptions = shared, CleanTestDir = false, ExtraScope = SecondScope };

        var refused = NodeAsyncAssert.ThrowsAsync<InvalidOperationException>(StartSecondAsync(secondOptions, cancellationToken));
        var ex = await refused;
        _ = await Assert.That(ex.Message).Contains("in use by another node");

        var cache = first["node-a"].GetCache<object?>(CacheName);
        var entry = new NodeCacheEntry<object?> { Value = "value-1", Version = 1 };
        await cache.SetEntryAsync(Guid.NewGuid().ToString("N"), CacheName, "held-dir-key", entry, cancellationToken);

        await first.StopNodeAsync("node-a");
        await JournalSegmentLeaseWait.WaitForReleasedAsync(dataDir, cancellationToken);
        await using var restarted = await StartClusterAsync("node-a", new IntegrationStartOptions { PersistenceOptions = shared, CleanTestDir = false, ExtraScope = SecondScope }, cancellationToken);

        _ = await Assert.That(restarted["node-a"].DataDir).IsEqualTo(dataDir);
    }

    /// <summary>A replicated host refused on a held directory creates nothing in it: no stamp, no temp file, no group storage.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task RefusedReplicatedHostLeavesNoFiles(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-data-dir-lock-replicated");
        using var held = DataDirectoryLock.Acquire(dir);
        var options = new IntegrationStartOptions
        {
            ReplicaCount = 2,
            PersistenceOptions = new PersistenceOptions { DataDir = dir, JournalMaxSegmentMb = 64 },
            CleanTestDir = false,
            ExtraScope = ReplicatedScope,
        };

        var refused = NodeAsyncAssert.ThrowsAsync<InvalidOperationException, TestCluster<IntegrationStartOptions>>(StartClusterAsync("node-a", "node-b", options, cancellationToken));
        var ex = await refused;

        _ = await Assert.That(ex.Message).Contains("another node");
        var entries = Directory.GetFileSystemEntries(dir, "*", SearchOption.AllDirectories);
        _ = await Assert.That(entries.Length).IsEqualTo(1);
        _ = await Assert.That(Path.GetFileName(entries[0])).IsEqualTo(DataDirectoryLock.LockFileName);
    }

    /// <summary>A start refused after the lock was taken releases it, so a correct start on the same directory succeeds.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task RefusedStartReleasesLock(CancellationToken cancellationToken)
    {
        var options = new IntegrationStartOptions { ReplicaCount = 2, UsePersistence = true, ExtraScope = RefusedScope };
        await using var cluster = await StartClusterAsync("node-a", "node-b", options, cancellationToken);
        var dataDir = cluster["node-a"].DataDir;
        await cluster.StopNodeAsync("node-a");
        await JournalSegmentLeaseWait.WaitForReleasedAsync(dataDir, cancellationToken);

        // An RF=1 start on a directory activated for replication is refused after the lock is taken.
        var wrong = new IntegrationStartOptions { ReplicaCount = 1, UsePersistence = true, CleanTestDir = false, ExtraScope = RefusedScope };
        _ = await NodeAsyncAssert.ThrowsAsync<InvalidOperationException, ITestNodeHost>(cluster.StartNodeAsync("node-a", wrong, cancellationToken));

        var right = new IntegrationStartOptions { ReplicaCount = 2, UsePersistence = true, CleanTestDir = false, ExtraScope = RefusedScope };
        var restarted = await cluster.StartNodeAsync("node-a", right, cancellationToken);

        _ = await Assert.That(restarted.DataDir).IsEqualTo(dataDir);
    }

    private async Task StartSecondAsync(IntegrationStartOptions options, CancellationToken cancellationToken)
    {
        await using var second = await StartClusterAsync("node-b", options, cancellationToken);
    }
}
