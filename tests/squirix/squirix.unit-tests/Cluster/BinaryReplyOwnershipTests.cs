using System;
using System.Net.Security;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Internal.Cluster.Transport;
using Squirix.Transport.Wire.Cache;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.UnitTests.Cluster;

/// <summary>Exercises the lifetime of an unread result after the connection accepts more work.</summary>
public sealed class BinaryReplyOwnershipTests
{
    /// <summary>A later lookup may reuse the wire ID before the caller consumes the first result.</summary>
    /// <param name="cancellationToken">The cancellation token for the test.</param>
    [Test]
    public Task UnreadReplySurvivesCorrelationIdReuse(CancellationToken cancellationToken) =>
        AsyncBinaryCompletionTests.WithPeerAsync(CheckUnreadReplyAsync, cancellationToken);

    private static async Task CheckUnreadReplyAsync(BinaryCacheClient client, SslStream peer, CancellationToken token)
    {
        var options = new BinaryCallOptions(DateTime.UtcNow.AddMinutes(1), token, token);
        var firstRequest = new GetDistributedAsyncRequest { CacheName = "items", Key = "first" };
        var nextRequest = new GetDistributedAsyncRequest { CacheName = "items", Key = "next" };
        var barrierRequest = new RemoveAsyncRequest { CacheName = "items", Key = "barrier" };
        var first = client.GetDistributedAsync(firstRequest, in options);
        var barrier = client.RemoveAsync(barrierRequest, in options);
        var firstId = await AsyncBinaryCompletionTests.ReadRequestAsync(peer, BinaryOperation.GetDistributed, firstRequest, token);
        var barrierId = await AsyncBinaryCompletionTests.ReadRequestAsync(peer, BinaryOperation.Remove, barrierRequest, token);
        var stored = new GetDistributedAsyncResponse { Found = true, Value = new byte[] { 37 } };
        await AsyncBinaryCompletionTests.ReplyAsync(peer, firstId, stored, token);
        await AsyncBinaryCompletionTests.ReplyAsync(peer, barrierId, new RemoveAsyncResponse { Removed = true }, token);
        _ = await Assert.That((await barrier).Removed).IsTrue();

        var next = client.GetDistributedAsync(nextRequest, in options);
        var nextId = await AsyncBinaryCompletionTests.ReadRequestAsync(peer, BinaryOperation.GetDistributed, nextRequest, token);
        _ = await Assert.That(nextId).IsEqualTo(firstId);
        await AsyncBinaryCompletionTests.ReplyAsync(peer, nextId, new GetDistributedAsyncResponse(), token);
        _ = await Assert.That((await next).Found).IsFalse();
        var original = await first;
        _ = await Assert.That(original.Found).IsTrue();
        _ = await Assert.That(original.Value.Span.SequenceEqual(stored.Value.Span)).IsTrue();
    }
}
