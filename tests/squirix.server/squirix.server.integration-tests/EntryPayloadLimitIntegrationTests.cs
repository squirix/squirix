using System;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.IntegrationTests.Support;
using Squirix.Server.TestKit.Hosting;
using Squirix.Server.TestKit.Networking;
using Squirix.Transport.Binary;
using Squirix.Transport.Wire;
using Squirix.Transport.Wire.Cache;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.IntegrationTests;

/// <summary>Exercises public admission and transport of large single-copy writes.</summary>
public sealed class EntryPayloadLimitIntegrationTests : NodeIntegrationTestBase
{
    private const string CacheName = "default";
    private const string NodeA = "node-a";
    private const string NodeB = "node-b";

    /// <summary>The ingress server answers an oversized header without waiting for the value.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task RejectsOversizedHeaderBeforeReadingValue(CancellationToken cancellationToken)
    {
        await using var cluster = await StartClusterAsync(NodeA, cancellationToken: cancellationToken);
        using var channel = CreateBinaryChannel(cluster[NodeA].Uri);
        var refusal = await channel.ReadWriteHeaderRejectionAsync(BinaryOperation.SetEntry, Frames.MaxValueBytes + 1, cancellationToken);

        _ = await Assert.That(refusal?.StatusCode).IsEqualTo(FrameStatus.ResourceExhausted);
        _ = await Assert.That(refusal!.Error.Detail).Contains("536870912", StringComparison.Ordinal);
    }

    /// <summary>Large requests and replies keep their complete value when routed through another node.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task ForwardedUpdatePreservesLargePayload(CancellationToken cancellationToken)
    {
        await using var cluster = await StartClusterAsync(NodeA, NodeB, cancellationToken: cancellationToken);
        var key = cluster[NodeB].FindKeyOwnedBy(CacheName, NodeB);
        using var channel = CreateBinaryChannel(cluster[NodeA].Uri);
        var client = new CacheWireClient(channel);
        _ = await client.SetEntryAsync(
            new SetEntryAsyncRequest
            {
                CacheName = CacheName,
                Key = key,
                Entry = new CacheEntryWire { Value = new CacheValue { StringValue = "original" } },
            },
            cancellationToken: cancellationToken);

        var data = new byte[9 * 1024 * 1024];
        data[^1] = 42;
        var update = await client.UpdateAsync(
            new UpdateAsyncRequest
            {
                CacheName = CacheName,
                Key = key,
                Entry = new CacheEntryWire { Value = CacheValue.FromOwnedBinary(data) },
            },
            cancellationToken: cancellationToken);

        _ = await Assert.That(update.Updated).IsTrue();
        var result = await client.GetValueAsync(new GetValueAsyncRequest { CacheName = CacheName, Key = key }, cancellationToken: cancellationToken);
        _ = await Assert.That(result.Found).IsTrue();
        _ = await Assert.That(result.Value!.BinaryValue.Span.SequenceEqual(data)).IsTrue();
    }

    /// <summary>A payload exactly at the limit survives the journal and recovery with its expiration envelope.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    [NotInParallel]
    public async Task RecoversMaximumSingleCopyValue(CancellationToken cancellationToken)
    {
        var persistence = new IntegrationStartOptions { UsePersistence = true, CleanTestDir = false };
        await using var cluster = await StartClusterAsync(NodeA, persistence, cancellationToken);
        using var channel = CreateBinaryChannel(cluster[NodeA].Uri);
        var client = new CacheWireClient(channel);
        var data = new byte[Frames.MaxValueBytes];
        data[0] = 7;
        data[^1] = 42;
        const string key = "maximum-single-copy";
        _ = await client.SetDistributedAsync(
            new SetDistributedAsyncRequest
            {
                CacheName = CacheName,
                Key = key,
                OperationId = RpcOperationIdentity.New(),
                Value = data,
                TtlTicks = TimeSpan.FromMinutes(5).Ticks,
            },
            cancellationToken: cancellationToken);

        var restarted = await cluster.RestartNodeAsync(NodeA, cancellationToken: cancellationToken);
        using var recoveryChannel = CreateBinaryChannel(restarted.Uri);
        var recoveryClient = new CacheWireClient(recoveryChannel);
        var result = await recoveryClient.GetDistributedAsync(new GetDistributedAsyncRequest { CacheName = CacheName, Key = key }, cancellationToken: cancellationToken);
        _ = await Assert.That(result.Found).IsTrue();
        _ = await Assert.That(result.Value.Span.SequenceEqual(data)).IsTrue();
    }
}
