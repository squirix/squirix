using System;
using System.Threading;
using System.Threading.Tasks;
using Grpc.Core;
using Microsoft.Extensions.Time.Testing;
using Squirix.Server.Cluster;
using Squirix.Server.IntegrationTests.Support;
using Squirix.Server.Node.Backpressure;
using Squirix.Server.TestKit;
using Squirix.Server.TestKit.Hosting;
using Squirix.Transport.Grpc.Cache;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.IntegrationTests;

/// <summary>Per-client backpressure limits follow the calling principal, not the node-to-node connection that forwards its requests.</summary>
public sealed class BackpressureForwardingTests : NodeIntegrationTestBase
{
    private const int Burst = 4;
    private const int MaxCalls = 20;

    /// <summary>
    /// Principals with a per-client rate limit are refused independently on the entry node, and the owner does not
    /// pool the forwarded requests of all principals into one bucket of the forwarding connection.
    /// </summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task ForwardedRequestsKeepPrincipalIsolation(CancellationToken cancellationToken)
    {
        var credentials = TestJwtHelper.CreateRandomCredentials("https://integration.squirix.test", "grpc-cache");
        var backpressure = new SquirixServerBackpressureOptions { PerClientRateLimitPerSecond = 1, PerClientRateLimitBurst = Burst };
        var options = new IntegrationStartOptions
        {
            BackpressureOptions = backpressure.ToAdmissionOptions(),
            Security = TestNodeSecurityOptions.FromJwtCredentials(credentials),
            TimeProvider = new FakeTimeProvider(),
        };
        await using var cluster = await StartClusterAsync("node-a", "node-b", options, cancellationToken);
        var key = TestKeyOwnerHelper.TwoNode.FindKeyOwnedBy("default", "node-b", "forwarded-backpressure");
        using var channel = CreateGrpcChannel(cluster["node-a"].Uri);
        var client = new SquirixCacheService.SquirixCacheServiceClient(channel);

        var admittedA = await CountAdmittedAsync(client, TestJwtHelper.CreateBearerToken(credentials, subject: "tenant-a"), key, cancellationToken);
        var admittedB = await CountAdmittedAsync(client, TestJwtHelper.CreateBearerToken(credentials, subject: "tenant-b"), key, cancellationToken);

        _ = await Assert.That(admittedA).IsEqualTo(Burst);
        _ = await Assert.That(admittedB).IsEqualTo(Burst);
    }

    /// <summary>
    /// A forwarded request that finds the owner without a free slot is refused at once with no queue wait, and the entry
    /// node gives its only slot back, so the same request is served once the owner has a free slot again.
    /// </summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task ForwardedRequestFailsFastOnFullOwner(CancellationToken cancellationToken)
    {
        var options = new IntegrationStartOptions
        {
            BackpressureOptions = new AdmissionOptions
            {
                MaxInFlight = 1,
                MaxQueue = 4,
                SlowdownThreshold = 1,
                MaxSlowdownDelay = TimeSpan.Zero,
                MaxQueueWait = TimeSpan.FromMinutes(1),
            },
            TimeProvider = new FakeTimeProvider(),
        };
        await using var cluster = await StartClusterAsync("node-a", "node-b", options, cancellationToken);
        var key = TestKeyOwnerHelper.TwoNode.FindKeyOwnedBy("default", "node-b", "forwarded-saturated");
        using var channel = CreateGrpcChannel(cluster["node-a"].Uri);
        var client = new SquirixCacheService.SquirixCacheServiceClient(channel);
        var request = new GetValueAsyncRequest { CacheName = "default", Key = key };
        var ownerGate = cluster["node-b"].GetRequiredService<IBackpressureGate>();

        var (held, heldLease) = await ownerGate.AcquireAsync("grpc", "get", "grpc:holder", cancellationToken);
        var refused = await NodeAsyncAssert.ThrowsAsync<RpcException>(
            client.GetValueAsync(request, cancellationToken: cancellationToken).ResponseAsync.WaitAsync(TimeSpan.FromSeconds(30), TimeProvider.System, cancellationToken));
        heldLease.Dispose();
        _ = await client.GetValueAsync(request, cancellationToken: cancellationToken).ResponseAsync.WaitAsync(TimeSpan.FromSeconds(30), TimeProvider.System, cancellationToken);

        _ = await Assert.That(held.IsAccepted).IsTrue();
        _ = await Assert.That(refused.StatusCode).IsEqualTo(StatusCode.ResourceExhausted);
        _ = await Assert.That(refused.Status.Detail).Contains("forwarded_no_slot");
    }

    /// <summary>
    /// A forwarded request that finds every per-owner forwarding permit of the entry node taken is refused at once as
    /// `peer_busy`, and is served again once the permit is free.
    /// </summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task ForwardedRequestFailsFastOnBusyPeer(CancellationToken cancellationToken)
    {
        var options = new IntegrationStartOptions
        {
            BackpressureOptions = new AdmissionOptions
            {
                MaxInFlight = 2,
                MaxQueue = 4,
                SlowdownThreshold = 2,
                MaxSlowdownDelay = TimeSpan.Zero,
                MaxQueueWait = TimeSpan.FromMinutes(1),
            },
            TimeProvider = new FakeTimeProvider(),
        };
        await using var cluster = await StartClusterAsync("node-a", "node-b", options, cancellationToken);
        var key = TestKeyOwnerHelper.TwoNode.FindKeyOwnedBy("default", "node-b", "forwarded-busy-peer");
        using var channel = CreateGrpcChannel(cluster["node-a"].Uri);
        var client = new SquirixCacheService.SquirixCacheServiceClient(channel);
        var request = new GetValueAsyncRequest { CacheName = "default", Key = key };
        var policy = cluster["node-a"].GetRequiredService<IServerClientPool>().PolicyFor("node-b");
        var entered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        var holder = policy.ExecuteAsync(
            (entered, release),
            static async (s, ct) =>
            {
                s.entered.SetResult();
                await s.release.Task.WaitAsync(Timeout.InfiniteTimeSpan, TimeProvider.System, ct);
                return 0;
            },
            cancellationToken).AsTask();
        await entered.Task.WaitAsync(TimeSpan.FromSeconds(30), TimeProvider.System, cancellationToken);
        var refused = await NodeAsyncAssert.ThrowsAsync<RpcException>(
            client.GetValueAsync(request, cancellationToken: cancellationToken).ResponseAsync.WaitAsync(TimeSpan.FromSeconds(30), TimeProvider.System, cancellationToken));
        release.SetResult();
        _ = await holder;
        _ = await client.GetValueAsync(request, cancellationToken: cancellationToken).ResponseAsync.WaitAsync(TimeSpan.FromSeconds(30), TimeProvider.System, cancellationToken);

        _ = await Assert.That(refused.StatusCode).IsEqualTo(StatusCode.ResourceExhausted);
        _ = await Assert.That(refused.Status.Detail).Contains("peer_busy");
    }

    private static async Task<int> CountAdmittedAsync(SquirixCacheService.SquirixCacheServiceClient client, string token, string key, CancellationToken cancellationToken)
    {
        var headers = new Metadata { { "authorization", $"Bearer {token}" } };
        var admitted = 0;
        while (admitted < MaxCalls)
        {
            try
            {
                _ = await client.GetValueAsync(new GetValueAsyncRequest { CacheName = "default", Key = key }, new CallOptions(headers, cancellationToken: cancellationToken));
                admitted++;
            }
            catch (RpcException ex) when (ex.StatusCode == StatusCode.ResourceExhausted)
            {
                break;
            }
        }

        return admitted;
    }
}
