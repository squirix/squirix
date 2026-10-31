using System;
using System.Threading;
using System.Threading.Tasks;
using Google.Protobuf;
using Google.Protobuf.WellKnownTypes;
using Squirix.Server.Core;
using Squirix.Server.IntegrationTests.Support;
using Squirix.Server.Serialization.Grpc;
using Squirix.Server.TestKit.Hosting;
using Squirix.Transport.Grpc;
using Squirix.Transport.Grpc.Cache;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.IntegrationTests.Cluster.Replication;

/// <summary>Every single-key mutation retried on an RF=3 group owner answers with the outcome of its first attempt and takes effect once.</summary>
public sealed class GroupReplayMatrixTests : NodeIntegrationTestBase
{
    private const string CacheName = "default";
    private const string Owner = "node-a";

    /// <summary>The mutations that commit through the group and are retried with the same operation id.</summary>
    public enum GroupMutation
    {
        /// <summary>Removes the entry.</summary>
        Remove = 0,

        /// <summary>Removes the expiration of the entry.</summary>
        RemoveExpiration = 1,

        /// <summary>Sets the entry.</summary>
        SetEntry = 2,

        /// <summary>Sets a new expiration on the entry.</summary>
        Touch = 3,

        /// <summary>Replaces the value of the entry.</summary>
        Update = 4,
    }

    /// <summary>A retry after an intervening write replays the first outcome and neither undoes the intervening write nor changes its expiration.</summary>
    /// <param name="mutation">The mutation under test.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    [Arguments(GroupMutation.Remove)]
    [Arguments(GroupMutation.RemoveExpiration)]
    [Arguments(GroupMutation.SetEntry)]
    [Arguments(GroupMutation.Touch)]
    [Arguments(GroupMutation.Update)]
    public async Task RetryReplaysFirstOutcome(GroupMutation mutation, CancellationToken cancellationToken)
    {
        var options = new IntegrationStartOptions { ReplicaCount = 3, UsePersistence = true, CleanTestDir = true, ExtraScope = $"group-replay-{mutation}" };
        await using var cluster = await StartClusterAsync(Owner, "node-b", "node-c", options, cancellationToken);
        await ReplicaGroupFollowers.AwaitVerifiedAsync(cluster[Owner], cancellationToken);
        using var channel = CreateGrpcChannel(cluster[Owner].Uri);
        var client = new SquirixCacheService.SquirixCacheServiceClient(channel);
        var key = cluster[Owner].FindKeyOwnedBy(CacheName, Owner);
        _ = await client.SetEntryAsync(CreateSet(key, "initial", TimeSpan.FromHours(1)), cancellationToken: cancellationToken);
        var operationId = RpcOperationIdentity.New();

        var first = await SendAsync(mutation, client, key, operationId, cancellationToken);
        _ = await client.SetEntryAsync(CreateSet(key, "intervening", null), cancellationToken: cancellationToken);
        var second = await SendAsync(mutation, client, key, operationId, cancellationToken);

        _ = await Assert.That(second.Span.SequenceEqual(first.Span)).IsTrue();
        var value = await client.GetValueAsync(new GetValueAsyncRequest { CacheName = CacheName, Key = key }, cancellationToken: cancellationToken);
        var expiration = await client.GetExpirationAsync(new GetExpirationAsyncRequest { CacheName = CacheName, Key = key }, cancellationToken: cancellationToken);
        _ = await Assert.That(value.Found).IsTrue();
        _ = await Assert.That(value.Value.StringValue).IsEqualTo("intervening");
        _ = await Assert.That(expiration.HasExpiration).IsFalse();
    }

    private static SetEntryAsyncRequest CreateSet(string key, string value, TimeSpan? expiration) => new()
    {
        OperationId = RpcOperationIdentity.New(),
        CacheName = CacheName,
        Key = key,
        Entry = new NodeCacheEntry<object?> { Value = value, Version = 1, Expiration = expiration }.MapToProto(),
    };

    private static async Task<ByteString> SendAsync(GroupMutation mutation, SquirixCacheService.SquirixCacheServiceClient client, string key, string operationId, CancellationToken cancellationToken)
    {
        var entry = new NodeCacheEntry<object?> { Value = "mutated", Version = 1 }.MapToProto();
        return mutation switch
        {
            GroupMutation.Remove => (await client.RemoveAsync(new RemoveAsyncRequest { OperationId = operationId, CacheName = CacheName, Key = key }, cancellationToken: cancellationToken)).ToByteString(),
            GroupMutation.RemoveExpiration => (await client.RemoveExpirationAsync(
                new RemoveExpirationAsyncRequest { OperationId = operationId, CacheName = CacheName, Key = key },
                cancellationToken: cancellationToken)).ToByteString(),
            GroupMutation.SetEntry => (await client.SetEntryAsync(new SetEntryAsyncRequest { OperationId = operationId, CacheName = CacheName, Key = key, Entry = entry }, cancellationToken: cancellationToken))
               .ToByteString(),
            GroupMutation.Touch => (await client.TouchAsync(
                new TouchAsyncRequest { OperationId = operationId, CacheName = CacheName, Key = key, Expiration = Duration.FromTimeSpan(TimeSpan.FromHours(2)) },
                cancellationToken: cancellationToken)).ToByteString(),
            GroupMutation.Update => (await client.UpdateAsync(new UpdateAsyncRequest { OperationId = operationId, CacheName = CacheName, Key = key, Entry = entry }, cancellationToken: cancellationToken))
               .ToByteString(),
            _ => throw new ArgumentOutOfRangeException(nameof(mutation), mutation, "Unsupported mutation."),
        };
    }
}
