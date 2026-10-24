using System;
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
        _ = await Assert.That(ex.Message).Contains("another node process");

        var cache = first["node-a"].GetCache<object?>(CacheName);
        var entry = new NodeCacheEntry<object?> { Value = "value-1", Version = 1 };
        await cache.SetEntryAsync(Guid.NewGuid().ToString("N"), CacheName, "held-dir-key", entry, cancellationToken);

        await first.StopNodeAsync("node-a");
        await JournalSegmentLeaseWait.WaitForReleasedAsync(dataDir, cancellationToken);
        await using var restarted = await StartClusterAsync("node-a", new IntegrationStartOptions { PersistenceOptions = shared, CleanTestDir = false, ExtraScope = SecondScope }, cancellationToken);

        _ = await Assert.That(restarted["node-a"].DataDir).IsEqualTo(dataDir);
    }

    private async Task StartSecondAsync(IntegrationStartOptions options, CancellationToken cancellationToken)
    {
        await using var second = await StartClusterAsync("node-b", options, cancellationToken);
    }
}
