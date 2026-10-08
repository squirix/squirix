using System;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Cluster.Replication;
using Squirix.Server.Core;
using Squirix.Server.Errors;
using Squirix.Server.IntegrationTests.Support;
using Squirix.Server.Node.Services;
using Squirix.Server.TestKit;
using Squirix.Server.TestKit.Hosting;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.IntegrationTests.Cluster.Replication;

/// <summary>An entry over the size limit on an RF=3 group is refused before its replicated append, so the group keeps accepting writes.</summary>
public sealed class ReplicatedEntrySizeTests : NodeIntegrationTestBase
{
    private const string CacheName = "replicated-entry-size";

    /// <summary>Bounds the verification of a fresh cluster.</summary>
    private static readonly TimeSpan VerificationBound = TimeSpan.FromSeconds(30);

    /// <summary>An oversized set sent to the owner fails with the size limit, appends nothing, and the next write commits.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task OversizedSetRefusedBeforeAppend(CancellationToken cancellationToken)
    {
        var options = new IntegrationStartOptions { ReplicaCount = 3, UsePersistence = true, CleanTestDir = true, ExtraScope = "replicated-entry-size" };
        await using var cluster = await StartClusterAsync("node-a", "node-b", "node-c", options, cancellationToken);
        var owner = cluster["node-a"];
        await VerifyAsync(owner, cancellationToken);
        var cache = owner.GetCache<object?>(CacheName);
        var key = owner.FindKeyOwnedBy(CacheName, "node-a");
        _ = owner.GetRequiredService<ReplicaGroupRegistry>().TryGetLog("node-a", out var log);
        var before = (await log!.GetStatusAsync(cancellationToken)).LastLogIndex;

        // Above the 4 MiB entry limit and below the gRPC message limit, so only the entry limit can refuse it.
        var oversized = new NodeCacheEntry<object?> { Value = new string('x', 5 * 1024 * 1024), Version = 1 };
        var refused = await NodeAsyncAssert.ThrowsAsync<SquirixException>(cache.SetEntryAsync(Guid.NewGuid().ToString("N"), CacheName, key, oversized, cancellationToken));
        var after = (await log.GetStatusAsync(cancellationToken)).LastLogIndex;
        await cache.SetEntryAsync(Guid.NewGuid().ToString("N"), CacheName, key, new NodeCacheEntry<object?> { Value = "small", Version = 1 }, cancellationToken);
        var stored = await cache.GetValueAsync(CacheName, key, cancellationToken);

        _ = await Assert.That(refused.Code).IsEqualTo(SquirixErrorCode.PayloadTooLarge);
        _ = await Assert.That(after).IsEqualTo(before);
        _ = await Assert.That(stored.Value).IsEqualTo("small");
    }

    private static async Task VerifyAsync(ITestNodeHost owner, CancellationToken cancellationToken)
    {
        using var deadline = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        deadline.CancelAfter(VerificationBound);
        var committer = ReplicaNodeCommitters.OwnCommitter(owner);
        while (await committer.VerifyReplicasAsync(deadline.Token) != ReplicaVerification.AllReady)
            await Task.Delay(TimeSpan.FromMilliseconds(100), TimeProvider.System, deadline.Token);
    }
}
