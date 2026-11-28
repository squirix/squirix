using System;
using System.Net;
using System.Threading;
using System.Threading.Tasks;
using Squirix.E2ETests.Fixtures;
using Squirix.Server.TestKit;
using Squirix.Server.TestKit.Hosting;
using Squirix.Server.TestKit.Networking;
using StackExchange.Redis;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.E2ETests.Cluster;

/// <summary>Exercises the Redis client facade against the routed and replicated cache.</summary>
public sealed class RespClusterTests
{
    private const string CacheName = "resp";
    private const string KeyOwner = "nodeB";
    private static readonly TimeSpan Expiry = TimeSpan.FromMinutes(5);

    /// <summary>Routes basic commands and conditional writes when only the ingress node exposes RESP.</summary>
    /// <param name="protocol">The negotiated protocol.</param>
    /// <param name="replicas">The number of copies of each key.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    [Arguments(RedisProtocol.Resp2, 1)]
    [Arguments(RedisProtocol.Resp2, 3)]
    [Arguments(RedisProtocol.Resp3, 3)]
    [Timeout(60_000)]
    public async Task RoutesRespCommandsToOwnerWithoutListener(RedisProtocol protocol, int replicas, CancellationToken cancellationToken)
    {
        const string ingress = "nodeA";
        var options = new MultiNodeStartOptions
        {
            ReplicaCount = replicas,
            EnableRespOnNode = static node => string.Equals(node, ingress, StringComparison.Ordinal),
        };
        await using var cluster = await HostedCluster.StartThreeNodeAsync(nameof(RoutesRespCommandsToOwnerWithoutListener), options, true, cancellationToken);
        _ = await Assert.That(cluster.Cluster[KeyOwner].GetRespUri()).IsNull();
        var key = KeyOwnerHelper.ThreeNode.FindKeyOwnedBy(CacheName, KeyOwner, "resp-routed");
        await using var connection = await ConnectAsync(cluster, ingress, protocol, cancellationToken);
        var cache = connection.GetDatabase();

        _ = await Assert.That(await cache.StringSetAsync(key, "missing", when: When.Exists)).IsFalse();
        _ = await Assert.That((await cache.StringGetAsync(key)).IsNull).IsTrue();
        _ = await Assert.That(await cache.StringSetAsync(key, "first")).IsTrue();
        _ = await Assert.That(await cache.StringSetAsync(key, "ignored", when: When.NotExists)).IsFalse();
        _ = await Assert.That(await cache.StringSetAsync(key, "replacement", Expiry, When.Exists)).IsTrue();
        await AssertValueAndExpiryAsync(cache, key, "replacement");

        var owner = await cluster.GetCacheAsync<byte[]>(CacheName, KeyOwner, cancellationToken);
        var entry = await owner.GetValueAsync(key, cancellationToken);
        _ = await Assert.That(entry.Found).IsTrue();
        _ = await Assert.That(entry.Value.AsSpan().SequenceEqual("replacement"u8)).IsTrue();
        _ = await Assert.That(await cache.StringSetAsync(key, "persistent", when: When.Exists)).IsTrue();
        _ = await Assert.That(await cache.KeyTimeToLiveAsync(key)).IsNull();
        _ = await Assert.That(await cache.KeyExpireAsync(key, Expiry)).IsTrue();
        _ = await Assert.That(await cache.KeyPersistAsync(key)).IsTrue();
        _ = await Assert.That(await cache.KeyTimeToLiveAsync(key)).IsNull();
        _ = await Assert.That(await cache.KeyDeleteAsync(key)).IsTrue();
        _ = await Assert.That(await cache.KeyDeleteAsync(key)).IsFalse();
        _ = await Assert.That(await cache.KeyExistsAsync(key)).IsFalse();
        _ = await Assert.That(await cache.StringSetAsync(key, "new", when: When.NotExists)).IsTrue();
    }

    /// <summary>Reports a lost owner response and preserves a newer value.</summary>
    /// <param name="replicas">The number of copies of each key.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    [Arguments(1)]
    [Arguments(3)]
    [Timeout(60_000)]
    public async Task LostRespReplyPreservesNewerWrite(int replicas, CancellationToken cancellationToken)
    {
        const string ingress = "nodeA";
        var probe = new LostForwardResponseProbe();
        var options = new MultiNodeStartOptions
        {
            ReplicaCount = replicas,
            EnableRespOnNode = static node => string.Equals(node, ingress, StringComparison.Ordinal),
            ServicesConfigure = (node, services) =>
            {
                if (string.Equals(node, ingress, StringComparison.Ordinal))
                    probe.Register(services);
            },
        };
        await using var cluster = await HostedCluster.StartThreeNodeAsync(nameof(LostRespReplyPreservesNewerWrite), options, true, cancellationToken);
        var owner = await cluster.GetCacheAsync<byte[]>(CacheName, KeyOwner, cancellationToken);
        var key = KeyOwnerHelper.ThreeNode.FindKeyOwnedBy(CacheName, KeyOwner, "resp-replay");
        var ownerNode = cluster.Cluster[KeyOwner];
        await ownerNode.SeedBinaryAsync(CacheName, key, new byte[] { 1 }, cancellationToken);
        probe.LoseOnce(key, async () =>
        {
            var applied = await owner.GetValueAsync(key, cancellationToken);
            _ = await Assert.That(applied.Found).IsTrue();
            _ = await Assert.That(applied.Value.AsSpan().SequenceEqual(new byte[] { 2 })).IsTrue();
            await ownerNode.SeedBinaryAsync(CacheName, key, new byte[] { 3 }, cancellationToken);
        });
        await using var connection = await ConnectAsync(cluster, ingress, RedisProtocol.Resp3, cancellationToken);

        var write = connection.GetDatabase().StringSetAsync(key, new byte[] { 2 }, Expiry, When.Exists);
        var error = await NodeAsyncAssert.ThrowsAsync<RedisServerException>(write, TimeSpan.FromSeconds(10), TimeProvider.System, cancellationToken);
        _ = await Assert.That(error.Message).IsEqualTo("ERR commit outcome unknown");
        _ = await Assert.That(probe.ForwardAttempts(key)).IsEqualTo(1);
        _ = await Assert.That(probe.OwnerExecutions(key)).IsEqualTo(1);
        var current = await owner.GetValueAsync(key, cancellationToken);
        _ = await Assert.That(current.Found).IsTrue();
        _ = await Assert.That(current.Value.AsSpan().SequenceEqual(new byte[] { 3 })).IsTrue();
    }

    /// <summary>The owner refuses an oversized RESP read before returning its binary value to the ingress.</summary>
    /// <param name="replicas">The number of copies of each key.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    [Arguments(1)]
    [Arguments(3)]
    [Timeout(60_000)]
    public async Task OwnerRejectsOversizedRespRead(int replicas, CancellationToken cancellationToken)
    {
        const string ingress = "nodeA";
        const int limit = 1024 * 1024;
        var probe = new LostForwardResponseProbe();
        var options = new MultiNodeStartOptions
        {
            ReplicaCount = replicas,
            EnableRespOnNode = static node => string.Equals(node, ingress, StringComparison.Ordinal),
            ServicesConfigure = (node, services) =>
            {
                if (string.Equals(node, ingress, StringComparison.Ordinal))
                    probe.Register(services);
            },
        };
        await using var cluster = await HostedCluster.StartThreeNodeAsync(nameof(OwnerRejectsOversizedRespRead), options, true, cancellationToken);
        var ownerNode = cluster.Cluster[KeyOwner];
        var boundary = KeyOwnerHelper.ThreeNode.FindKeyOwnedBy(CacheName, KeyOwner, "resp-read-boundary");
        var oversized = KeyOwnerHelper.ThreeNode.FindKeyOwnedBy(CacheName, KeyOwner, "resp-read-oversized");
        var bytes = new byte[limit + 1];
        bytes[0] = 73;
        await ownerNode.SeedBinaryAsync(CacheName, boundary, bytes.AsMemory(0, limit), cancellationToken);
        await ownerNode.SeedBinaryAsync(CacheName, oversized, bytes, cancellationToken);
        probe.LoseOnce(oversized, static () => Task.CompletedTask);
        await using var connection = await ConnectAsync(cluster, ingress, RedisProtocol.Resp3, cancellationToken);
        var cache = connection.GetDatabase();

        var atLimit = (byte[]?)await cache.StringGetAsync(boundary);
        _ = await Assert.That(atLimit.AsSpan().SequenceEqual(bytes.AsSpan(0, limit))).IsTrue();
        var oversizedRead = cache.StringGetAsync(oversized);
        var error = await NodeAsyncAssert.ThrowsAsync<RedisServerException>(oversizedRead, TimeSpan.FromSeconds(10), TimeProvider.System, cancellationToken);
        _ = await Assert.That(error.Message).IsEqualTo("ERR cached value exceeds the RESP value limit");
        _ = await Assert.That(probe.ForwardAttempts(oversized)).IsEqualTo(1);
        _ = await Assert.That(probe.OwnerExecutions(oversized)).IsEqualTo(0);

        var sdk = await cluster.GetCacheAsync<byte[]>(CacheName, ingress, cancellationToken);
        var native = await sdk.GetValueAsync(oversized, cancellationToken);
        _ = await Assert.That(native.Found).IsTrue();
        _ = await Assert.That(native.Value.AsSpan().SequenceEqual(bytes)).IsTrue();
        _ = await Assert.That(probe.OwnerExecutions(oversized)).IsEqualTo(1);
    }

    /// <summary>A quorum-committed conditional replacement is visible after the original leader stops.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    [Timeout(90_000)]
    [ParallelLimiter<FailoverLimit>]
    public async Task RespReplacementSurvivesLeaderLoss(CancellationToken cancellationToken)
    {
        const string testName = nameof(RespReplacementSurvivesLeaderLoss);
        var options = new MultiNodeStartOptions
        {
            ReplicaCount = 3,
            Failover = true,
            ElectionTiming = FailoverTiming.For(testName),
            EnableRespOnNode = static _ => true,
        };
        await using var cluster = await HostedCluster.StartThreeNodeAsync(testName, options, true, cancellationToken);
        var probe = new ClusterLeaderProbe<ClusterStartOptions>(cluster.Cluster);
        var (leader, previousTerm) = await probe.WaitForStableLeaderAsync(KeyOwner, FailoverSteps.ThreeNodes, FailoverSteps.Bound, cancellationToken);
        var survivors = FailoverSteps.Except(FailoverSteps.ThreeNodes, leader);
        var key = KeyOwnerHelper.ThreeNode.FindKeyOwnedBy(CacheName, KeyOwner, "resp-failover");
        await using var connection = await ConnectAsync(cluster, survivors[0], RedisProtocol.Resp3, cancellationToken);
        var cache = connection.GetDatabase();
        _ = await Assert.That(await cache.StringSetAsync(key, "before-loss")).IsTrue();
        _ = await Assert.That(await cache.StringSetAsync(key, "committed", Expiry, When.Exists)).IsTrue();

        await cluster.AbruptShutdownNodeAsync(leader);
        await cluster.StopNodeAsync(leader);
        var (_, term) = await probe.WaitForStableLeaderAsync(KeyOwner, survivors, FailoverSteps.Bound, cancellationToken);
        _ = await Assert.That(term).IsGreaterThan(previousTerm);
        await AssertValueAndExpiryAsync(cache, key, "committed");
    }

    private static Task<ConnectionMultiplexer> ConnectAsync(HostedCluster cluster, string node, RedisProtocol protocol, CancellationToken cancellationToken)
    {
        var endpoint = cluster.Cluster[node].GetRespUri()!;
        var options = new ConfigurationOptions
        {
            EndPoints = { { IPAddress.Loopback, endpoint.Port } },
            AbortOnConnectFail = true,
            Ssl = true,
            SslHost = "localhost",
            Protocol = protocol,
        };
        if (LoopbackHttp.CreateTlsOptions().RemoteCertificateValidationCallback is { } validate)
            options.CertificateValidation += validate;

        return ConnectionMultiplexer.ConnectAsync(options).WaitAsync(cancellationToken);
    }

    private static async Task AssertValueAndExpiryAsync(IDatabase cache, string key, RedisValue expected)
    {
        _ = await Assert.That(await cache.StringGetAsync(key)).IsEqualTo(expected);
        var ttl = await cache.KeyTimeToLiveAsync(key);
        _ = await Assert.That(ttl > TimeSpan.Zero && ttl <= Expiry).IsTrue();
    }
}
