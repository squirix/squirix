using System;
using System.Net;
using System.Threading.Tasks;
using Squirix.Server.Core;
using Squirix.Server.IntegrationTests.Support;
using Squirix.Server.TestKit;
using Xunit;

namespace Squirix.Server.IntegrationTests.Ops;

/// <summary>Integration tests for journal recovery readiness and cache behavior during non-blocking recovery.</summary>
public sealed class JournalRecoveryReadinessIntegrationTests : IntegrationTestBase
{
    private const string NodeId = "node_recovery_gate";
    private const string Scope = "journal-recovery-gate";
    private const string PersistedKey = "recovery:persisted";
    private const string DuringRecoveryKey = "recovery:during";

    /// <summary>
    /// Ensures non-blocking recovery keeps <c>/health/ready</c> unhealthy until replay completes,
    /// cache reads stay empty until replay, and durable writes wait for the startup gate.
    /// </summary>
    [Fact]
    public async Task NonBlockingRecoveryKeepsReadyUnhealthyUntilGateOpensAndGatesCacheWrites()
    {
        var httpUri = GetNextHttpUri();

        await using (var seedNode = await StartNodeAsync(httpUri, NodeId, usePersistence: true, extraScope: Scope))
        {
            var seedCache = GetCache(seedNode);
            await seedCache.SetEntryAsync(TestOperationIds.Default, ServerCacheNames.DefaultNamespace, PersistedKey, BuildEntry("persisted-value"), DefaultCancellationToken);
        }

        var restartUrl = GetNextHttpUri();
        var replayDelay = new RecoveryReplayDelaySignal();

        await using var node = await StartNodeAsync(
            restartUrl,
            NodeId,
            servicesConfigure: services => RecoveryReplayTestRegistration.AddDelayedReplay(services, replayDelay),
            usePersistence: true,
            cleanTestDir: false,
            extraScope: Scope,
            waitForRecovery: false);

        Assert.Equal(HttpStatusCode.ServiceUnavailable, await GetReadyStatusCodeAsync(node.Uri));
        Assert.Equal(HttpStatusCode.OK, await GetLiveStatusCodeAsync(node.Uri));

        var cache = GetCache(node);
        var beforeReplay = await cache.GetValueAsync(ServerCacheNames.DefaultNamespace, PersistedKey, DefaultCancellationToken);
        Assert.False(beforeReplay.Found);

        var writeTask = cache.SetEntryAsync(TestOperationIds.Default, ServerCacheNames.DefaultNamespace, DuringRecoveryKey, BuildEntry("during-recovery"), DefaultCancellationToken).AsTask();
        var writeStarted = await Task.WhenAny(writeTask, Task.Delay(TimeSpan.FromMilliseconds(250), TimeProvider.System, DefaultCancellationToken));
        Assert.NotSame(writeTask, writeStarted);

        replayDelay.Release();

        await writeTask.WaitAsync(TimeSpan.FromSeconds(10), TimeProvider.System, DefaultCancellationToken);
        await WaitForReadyHealthyAsync(node.Uri);

        var recovered = await cache.GetValueAsync(ServerCacheNames.DefaultNamespace, PersistedKey, DefaultCancellationToken);
        Assert.True(recovered.Found);
        Assert.Equal("persisted-value", recovered.Value);

        var writtenDuringRecovery = await cache.GetValueAsync(ServerCacheNames.DefaultNamespace, DuringRecoveryKey, DefaultCancellationToken);
        Assert.True(writtenDuringRecovery.Found);
        Assert.Equal("during-recovery", writtenDuringRecovery.Value);
    }

    private async Task<HttpStatusCode> GetLiveStatusCodeAsync(Uri uri)
    {
        using var response = await HttpClient.GetAsync(new Uri(uri, "/health/live"), DefaultCancellationToken);
        return response.StatusCode;
    }

    private async Task<HttpStatusCode> GetReadyStatusCodeAsync(Uri uri)
    {
        using var response = await HttpClient.GetAsync(new Uri(uri, "/health/ready"), DefaultCancellationToken);
        return response.StatusCode;
    }

    private async Task WaitForReadyHealthyAsync(Uri uri)
    {
        var deadline = DateTime.UtcNow + TimeSpan.FromSeconds(10);
        while (DateTime.UtcNow < deadline)
        {
            if (await GetReadyStatusCodeAsync(uri) is HttpStatusCode.OK)
                return;

            await Task.Delay(TimeSpan.FromMilliseconds(50), TimeProvider.System, DefaultCancellationToken);
        }

        throw new TimeoutException("Timed out waiting for /health/ready to become healthy.");
    }
}
