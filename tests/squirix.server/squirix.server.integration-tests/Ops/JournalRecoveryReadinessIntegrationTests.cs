using System;
using System.Net;
using System.Threading.Tasks;
using Squirix.Server.Cluster.Membership;
using Squirix.Server.Core;
using Squirix.Server.IntegrationTests.Support;
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
        var seedUrl = GetNextHttpUri();
        var seedPeers = new[] { new Peer { NodeId = NodeId, Url = seedUrl.AbsoluteUri } };

        await using (var seedNode = await StartNodeAsync(seedUrl, seedPeers, usePersistence: true, extraScope: Scope))
        {
            var seedCache = GetCache(seedNode);
            await seedCache.SetAsync(CacheNames.DefaultNamespace, PersistedKey, BuildEntry("persisted-value"), DefaultCancellationToken);
        }

        var restartUrl = GetNextHttpUri();
        var restartPeers = new[] { new Peer { NodeId = NodeId, Url = restartUrl.AbsoluteUri } };
        var replayDelay = new RecoveryReplayDelaySignal();

        await using var node = await StartNodeAsync(
            restartUrl,
            restartPeers,
            usePersistence: true,
            cleanTestDir: false,
            extraScope: Scope,
            waitForRecovery: false,
            servicesConfigure: services => RecoveryReplayTestRegistration.AddDelayedReplay(services, replayDelay));

        Assert.Equal(HttpStatusCode.ServiceUnavailable, await GetReadyStatusCodeAsync(node.Address));
        Assert.Equal(HttpStatusCode.OK, await GetLiveStatusCodeAsync(node.Address));

        var cache = GetCache(node);
        var beforeReplay = await cache.TryGetValueAsync(CacheNames.DefaultNamespace, PersistedKey, DefaultCancellationToken);
        Assert.False(beforeReplay.Found);

        var writeTask = cache.SetAsync(CacheNames.DefaultNamespace, DuringRecoveryKey, BuildEntry("during-recovery"), DefaultCancellationToken).AsTask();
        var writeStarted = await Task.WhenAny(writeTask, Task.Delay(TimeSpan.FromMilliseconds(250), TimeProvider.System, DefaultCancellationToken));
        Assert.NotSame(writeTask, writeStarted);

        replayDelay.Release();

        await writeTask.WaitAsync(TimeSpan.FromSeconds(10), TimeProvider.System, DefaultCancellationToken);
        await WaitForReadyHealthyAsync(node.Address);

        var recovered = await cache.GetValueAsync(CacheNames.DefaultNamespace, PersistedKey, DefaultCancellationToken);
        Assert.Equal("persisted-value", recovered);

        var writtenDuringRecovery = await cache.GetValueAsync(CacheNames.DefaultNamespace, DuringRecoveryKey, DefaultCancellationToken);
        Assert.Equal("during-recovery", writtenDuringRecovery);
    }

    private async Task<HttpStatusCode> GetLiveStatusCodeAsync(string address)
    {
        using var response = await HttpClient.GetAsync(new Uri(new Uri(address, UriKind.Absolute), "/health/live"), DefaultCancellationToken);
        return response.StatusCode;
    }

    private async Task<HttpStatusCode> GetReadyStatusCodeAsync(string address)
    {
        using var response = await HttpClient.GetAsync(new Uri(new Uri(address, UriKind.Absolute), "/health/ready"), DefaultCancellationToken);
        return response.StatusCode;
    }

    private async Task WaitForReadyHealthyAsync(string address)
    {
        var deadline = DateTime.UtcNow + TimeSpan.FromSeconds(10);
        while (DateTime.UtcNow < deadline)
        {
            if (await GetReadyStatusCodeAsync(address) is HttpStatusCode.OK)
                return;

            await Task.Delay(TimeSpan.FromMilliseconds(50), TimeProvider.System, DefaultCancellationToken);
        }

        throw new TimeoutException("Timed out waiting for /health/ready to become healthy.");
    }
}
