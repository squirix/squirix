using System;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Cluster.Replication;
using Squirix.Server.Core;
using Squirix.Server.IntegrationTests.Support;
using Squirix.Server.Node.Hosting;
using Squirix.Server.Node.Services;
using Squirix.Server.TestKit;
using Squirix.Server.TestKit.Hosting;
using Squirix.Server.TestKit.Networking;
using Squirix.Transport.Binary;
using Squirix.Transport.Wire;
using Squirix.Transport.Wire.Cache;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.IntegrationTests.Cluster.Replication;

/// <summary>Checks large forwarded writes through a real RF=3 commit and follower apply.</summary>
public sealed class ReplicatedEntrySizeTests : NodeIntegrationTestBase
{
    private const string CacheName = "replicated-entry-size";
    private const string Owner = "node-a";
    private const string Ingress = "node-b";
    private const string Follower = "node-c";

    /// <summary>A value at the limit reaches the owner and follower, while an oversized write cannot append.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task ForwardedLimitValueReplicates(CancellationToken cancellationToken)
    {
        var options = new IntegrationStartOptions { ReplicaCount = 3, UsePersistence = true, CleanTestDir = true };
        await using var cluster = await StartClusterAsync(Owner, Ingress, Follower, options, cancellationToken);
        var owner = cluster[Owner];
        await ReplicaGroupFollowers.AwaitVerifiedAsync(owner, cancellationToken);
        var key = owner.FindKeyOwnedBy(CacheName, Owner);
        _ = owner.GetRequiredService<ReplicaGroupRegistry>().TryGetLog(Owner, out var log);
        var before = (await log!.GetStatusAsync(cancellationToken)).LastLogIndex;
        using var channel = CreateBinaryChannel(cluster[Ingress].Uri);

        var refusal = await channel.ReadWriteHeaderRejectionAsync(BinaryOperation.SetEntry, EntryLimits.MaxReplicatedValueBytes + 1, cancellationToken);

        _ = await Assert.That(refusal?.StatusCode).IsEqualTo(FrameStatus.ResourceExhausted);
        _ = await Assert.That(refusal!.Error.Detail).Contains("33554432", StringComparison.Ordinal);
        _ = await Assert.That((await log.GetStatusAsync(cancellationToken)).LastLogIndex).IsEqualTo(before);

        var data = new byte[EntryLimits.MaxReplicatedValueBytes];
        data[0] = 7;
        data[^1] = 42;
        var client = new CacheWireClient(channel);
        _ = await client.SetEntryAsync(
            new SetEntryAsyncRequest
            {
                OperationId = RpcOperationIdentity.New(),
                CacheName = CacheName,
                Key = key,
                Entry = new CacheEntryWire { Value = CacheValue.FromOwnedBinary(data), Expiration = TimeSpan.FromMinutes(5) },
            },
            cancellationToken: cancellationToken);

        var committed = (await log.GetStatusAsync(cancellationToken)).CommitIndex;
        _ = await Assert.That(committed).IsGreaterThan(before);
        var follower = cluster[Follower];
        await ReplicaGroupFollowers.AwaitCaughtUpAsync(owner, Owner, [(Follower, follower)], cancellationToken);
        await AwaitFollowerApplyAsync(follower, committed, cancellationToken);
        var copy = await follower.GetRequiredService<OwnerLocalCache>().Value.GetEntryAsync(CacheName, key, cancellationToken);
        var bytes = StoredValueProbe.Binary(copy?.Value);
        _ = await Assert.That(bytes.AsSpan().SequenceEqual(data)).IsTrue();
    }

    private static async Task AwaitFollowerApplyAsync(ITestNodeHost follower, ulong index, CancellationToken cancellationToken)
    {
        using var deadline = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        deadline.CancelAfter(TimeSpan.FromSeconds(30));
        var applier = follower.GetRequiredService<ReplicaGroupAppliers>().For(Owner);
        while (applier.AppliedIndex < index)
            await Task.Delay(TimeSpan.FromMilliseconds(20), TimeProvider.System, deadline.Token);
    }
}
