using System;
using System.Net;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Core;
using Squirix.Server.IntegrationTests.Support;
using Squirix.Server.Runtime.Contracts;
using Squirix.Server.TestKit;
using Squirix.Server.TestKit.Hosting;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.IntegrationTests;

/// <summary>Integration tests for journal recovery readiness and cache behavior during non-blocking recovery.</summary>
public sealed class JournalRecoveryReadinessIntegrationTests : NodeIntegrationTestBase
{
    private const string DuringRecoveryKey = "recovery:during";
    private const string NodeId = "node_recovery_gate";
    private const string PersistedKey = "recovery:persisted";
    private const string Scope = "journal-recovery-gate";

    /// <summary>
    /// Ensures non-blocking recovery keeps <c language="csharp">/health/ready</c> unhealthy until replay completes,
    /// cache reads stay empty until replay, and durable writes wait for the startup gate.
    /// </summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task NonBlockingRecoveryUnblocksWrites(CancellationToken cancellationToken)
    {
        await SeedPersistedEntryAsync(cancellationToken);

        var restartUrl = GetNextHttpUri();
        var replayDelay = new RecoveryReplayDelaySignal();
        await using var cluster = await StartDelayedReplayNodeAsync(restartUrl, replayDelay, cancellationToken);
        var node = cluster[NodeId];

        try
        {
            await AssertBlockedUntilReplayAsync(node, replayDelay, cancellationToken);
            await AssertRecoveredStateAsync(GetCache(node), cancellationToken);
        }
        finally
        {
            replayDelay.Release();
        }
    }

    private static async Task AssertRecoveredStateAsync(ILogicalNamespacedCache<object?> cache, CancellationToken cancellationToken)
    {
        var recovered = await cache.GetValueAsync(ServerCacheNames.DefaultNamespace, PersistedKey, cancellationToken);
        _ = await Assert.That(recovered.Found).IsTrue();
        _ = await Assert.That(recovered.Value).IsEqualTo("persisted-value");

        var writtenDuringRecovery = await cache.GetValueAsync(ServerCacheNames.DefaultNamespace, DuringRecoveryKey, cancellationToken);
        _ = await Assert.That(writtenDuringRecovery.Found).IsTrue();
        _ = await Assert.That(writtenDuringRecovery.Value).IsEqualTo("during-recovery");
    }

    private async Task AssertBlockedUntilReplayAsync(ITestNodeHost node, RecoveryReplayDelaySignal replayDelay, CancellationToken cancellationToken)
    {
        _ = await Assert.That(await GetReadyStatusCodeAsync(node.Uri, cancellationToken)).IsEqualTo(HttpStatusCode.ServiceUnavailable);
        _ = await Assert.That(await GetLiveStatusCodeAsync(node.Uri, cancellationToken)).IsEqualTo(HttpStatusCode.OK);

        var cache = GetCache(node);
        var beforeReplay = await cache.GetValueAsync(ServerCacheNames.DefaultNamespace, PersistedKey, cancellationToken);
        _ = await Assert.That(beforeReplay.Found).IsFalse();

        var entry = BuildEntry("during-recovery");
        var writeTask = cache.SetEntryAsync(IntegrationMutationOpIds.Default, ServerCacheNames.DefaultNamespace, DuringRecoveryKey, entry, cancellationToken).AsTask();
        var writeStarted = await Task.WhenAny(writeTask, Task.Delay(TimeSpan.FromMilliseconds(250), TimeProvider.System, cancellationToken));
        _ = await Assert.That(writeStarted).IsNotSameReferenceAs(writeTask);

        replayDelay.Release();
        await writeTask.WaitAsync(TimeSpan.FromSeconds(10), TimeProvider.System, cancellationToken);
        await node.Uri.WaitUntilValueAsync(async (uri, token) => await GetReadyStatusCodeAsync(uri, token) == HttpStatusCode.OK, TimeSpan.FromSeconds(10), cancellationToken);
    }

    private async Task<HttpStatusCode> GetLiveStatusCodeAsync(Uri uri, CancellationToken cancellationToken)
    {
        using var response = await HttpClient.GetAsync(new Uri(uri, "/health/live"), cancellationToken);
        return response.StatusCode;
    }

    private async Task<HttpStatusCode> GetReadyStatusCodeAsync(Uri uri, CancellationToken cancellationToken)
    {
        using var response = await HttpClient.GetAsync(new Uri(uri, "/health/ready"), cancellationToken);
        return response.StatusCode;
    }

    private async Task SeedPersistedEntryAsync(CancellationToken cancellationToken)
    {
        await using var seedCluster = await StartClusterAsync(NodeId, new IntegrationStartOptions { UsePersistence = true, ExtraScope = Scope }, cancellationToken);
        var seedCache = GetCache(seedCluster[NodeId]);
        await seedCache.SetEntryAsync(IntegrationMutationOpIds.Default, ServerCacheNames.DefaultNamespace, PersistedKey, BuildEntry("persisted-value"), cancellationToken);
    }

    private ValueTask<TestCluster<IntegrationStartOptions>> StartDelayedReplayNodeAsync(Uri restartUrl, RecoveryReplayDelaySignal replayDelay, CancellationToken cancellationToken)
    {
        var options = new IntegrationStartOptions
        {
            ServicesConfigure = RecoveryReplayTestRegistration.CreateDelayedReplayConfigure(replayDelay),
            UsePersistence = true,
            CleanTestDir = false,
            ExtraScope = Scope,
            WaitForRecovery = false,
        };
        return StartClusterAsync(new ClusterNode(NodeId, restartUrl), options, cancellationToken);
    }
}
