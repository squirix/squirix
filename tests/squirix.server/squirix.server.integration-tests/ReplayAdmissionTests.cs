using System;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Time.Testing;
using Squirix.Server.IntegrationTests.Support;
using Squirix.Server.Node.Backpressure;
using Squirix.Server.TestKit;
using Squirix.Server.TestKit.Hosting;
using Squirix.Server.TestKit.Networking;
using Squirix.Transport.Binary;
using Squirix.Transport.Grpc.Cache;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.IntegrationTests;

/// <summary>Real gRPC replay requests obey admission before reaching the retained outcome.</summary>
public sealed class ReplayAdmissionTests : NodeIntegrationTestBase
{
    /// <summary>Replays consume rate tokens, while a fresh mutation consumes exactly one token.</summary>
    /// <param name="cancellationToken">Test cancellation token.</param>
    [Test]
    public async Task ReplayObeysRateLimitTest(CancellationToken cancellationToken)
    {
        var clock = new FakeTimeProvider();
        var options = new IntegrationStartOptions
        {
            TimeProvider = clock,
            BackpressureOptions = new AdmissionOptions { NodeRateLimitPerSecond = 1, NodeRateLimitBurst = 1 },
        };
        await using var cluster = await StartClusterAsync("replay-rate", options, cancellationToken);
        using var channel = CreateBinaryChannel(cluster["replay-rate"].Uri);
        var client = new CacheWireClient(channel);
        var request = new TryAddEntryAsyncRequest
        {
            OperationId = "0123456789abcdef0123456789abcdef",
            CacheName = "default",
            Key = "replay-rate",
            Entry = new CacheEntryWire { Value = new CacheValue { StringValue = "value" } },
        };
        var first = await client.AddEntryIfAbsentAsync(request, cancellationToken: cancellationToken);
        _ = await Assert.That(first.Added).IsTrue();
        var error = await NodeAsyncAssert.ThrowsAsync<ServerCallException>(
            client.AddEntryIfAbsentAsync(request, cancellationToken: cancellationToken),
            TimeSpan.FromSeconds(10),
            TimeProvider.System,
            cancellationToken);
        _ = await Assert.That(error.StatusCode).IsEqualTo(FrameStatus.ResourceExhausted);
        _ = await Assert.That(error.Message).Contains("node_rate_limit");

        clock.Advance(TimeSpan.FromSeconds(1));
        var replay = await client.AddEntryIfAbsentAsync(request, cancellationToken: cancellationToken);
        _ = await Assert.That(replay.Added).IsTrue();
    }

    /// <summary>A cached success cannot bypass a saturated gate; a retry after release still replays the original result.</summary>
    /// <param name="persistent">Whether the node journals mutations.</param>
    /// <param name="cancellationToken">Test cancellation token.</param>
    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task ReplayObeysCapacityTest(bool persistent, CancellationToken cancellationToken)
    {
        var options = new IntegrationStartOptions
        {
            UsePersistence = persistent,
            BackpressureOptions = new AdmissionOptions { MaxInFlight = 1, MaxQueue = 0, SlowdownThreshold = 1, MaxSlowdownDelay = TimeSpan.Zero },
        };
        await using var cluster = await StartClusterAsync("replay-admission", options, cancellationToken);
        var host = cluster["replay-admission"];
        using var channel = CreateBinaryChannel(host.Uri);
        var client = new CacheWireClient(channel);
        var request = new TryAddEntryAsyncRequest
        {
            OperationId = "0123456789abcdef0123456789abcdef",
            CacheName = "default",
            Key = "replay-admission",
            Entry = new CacheEntryWire { Value = new CacheValue { StringValue = "value" } },
        };
        var first = await client.AddEntryIfAbsentAsync(request, cancellationToken: cancellationToken);
        _ = await Assert.That(first.Added).IsTrue();

        var gate = host.GetRequiredService<IBackpressureGate>();
        var (admitted, lease) = await gate.AcquireAsync("cache", "set", "test", cancellationToken);
        using (lease)
        {
            _ = await Assert.That(admitted.IsAccepted).IsTrue();
            var error = await NodeAsyncAssert.ThrowsAsync<ServerCallException>(
                client.AddEntryIfAbsentAsync(request, cancellationToken: cancellationToken),
                TimeSpan.FromSeconds(10),
                TimeProvider.System,
                cancellationToken);
            _ = await Assert.That(error.StatusCode).IsEqualTo(FrameStatus.ResourceExhausted);
        }

        var replay = await client.AddEntryIfAbsentAsync(request, cancellationToken: cancellationToken);
        _ = await Assert.That(replay.Added).IsTrue();
    }
}
