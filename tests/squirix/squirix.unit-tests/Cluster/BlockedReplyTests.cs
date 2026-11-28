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

/// <summary>Checks progress on a connection while user code holds a reply continuation.</summary>
public sealed class BlockedReplyTests
{
    /// <summary>Delivers another reply through the same connection before the first caller resumes.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public Task BlockedCallerAllowsAnotherReply(CancellationToken cancellationToken) =>
        AsyncBinaryCompletionTests.WithPeerAsync(CheckDeliveryAsync, cancellationToken);

    private static async Task CheckDeliveryAsync(BinaryCacheClient client, SslStream peer, CancellationToken token)
    {
        var options = new BinaryCallOptions(DateTime.UtcNow.AddMinutes(1), token, token);
        var first = new RemoveAsyncRequest { CacheName = "items", Key = "first" };
        var second = new RemoveAsyncRequest { CacheName = "items", Key = "second" };
        var firstReply = client.RemoveAsync(first, in options);
        var secondReply = client.RemoveAsync(second, in options).AsTask();
        var firstId = await AsyncBinaryCompletionTests.ReadRequestAsync(peer, BinaryOperation.Remove, first, token);
        var secondId = await AsyncBinaryCompletionTests.ReadRequestAsync(peer, BinaryOperation.Remove, second, token);
        using var release = new ManualResetEventSlim();
        var entered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var blocked = ObserveAsync(firstReply, entered, release.Wait, token);
        try
        {
            await AsyncBinaryCompletionTests.ReplyAsync(peer, firstId, new RemoveAsyncResponse { Removed = true }, token);
            await entered.Task.WaitAsync(token);
            await AsyncBinaryCompletionTests.ReplyAsync(peer, secondId, new RemoveAsyncResponse { Removed = false }, token);
            _ = await Assert.That((await secondReply.WaitAsync(token)).Removed).IsFalse();
        }
        finally
        {
            release.Set();
            await blocked;
        }
    }

    private static async Task ObserveAsync(ValueTask<RemoveAsyncResponse> reply, TaskCompletionSource entered, Action<CancellationToken> wait, CancellationToken token)
    {
        var result = await reply.ConfigureAwait(false);
        entered.SetResult();
        wait(token);
        _ = await Assert.That(result.Removed).IsTrue();
    }
}
