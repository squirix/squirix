using System;
using System.Buffers;
using System.Net.Security;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Internal.Cluster.Transport;
using Squirix.TestKit;
using Squirix.Transport;
using Squirix.Transport.Binary;
using Squirix.Transport.Wire.Cache;
using Squirix.Transport.Wire.Codec;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.UnitTests.Cluster;

/// <summary>Checks per-attempt timeouts on a shared binary connection.</summary>
public sealed class BinaryDeadlineTests
{
    /// <summary>The timeout may retire the connection only if it wins admission against the next call.</summary>
    /// <param name="enterFirst">Whether the next call enters before the timeout retires the last call.</param>
    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task TimeoutAndNewAdmissionAreExclusive(bool enterFirst)
    {
        BinaryConnectionLifetime lifetime = default;
        _ = await Assert.That(lifetime.TryEnter()).IsTrue();
        lifetime.Leave();
        if (enterFirst)
            _ = await Assert.That(lifetime.TryEnter()).IsTrue();

        _ = await Assert.That(lifetime.TryCloseIfIdle()).IsEqualTo(!enterFirst);
        _ = await Assert.That(lifetime.IsOpen).IsEqualTo(enterFirst);
        if (enterFirst)
        {
            lifetime.Leave();
            _ = await Assert.That(lifetime.TryClose()).IsTrue();
        }

        _ = await Assert.That(lifetime.TryEnter()).IsFalse();
        _ = await Assert.That(lifetime.TryClose()).IsFalse();
    }

    /// <summary>Rejects an oversized single-copy write even if its attempt expires while the sender measures it.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public Task ExpiredOversizedWriteKeepsItsSizeError(CancellationToken cancellationToken) =>
        AsyncBinaryCompletionTests.WithPeerAsync(CheckOversizedWriteAsync, cancellationToken, legacy: true);

    /// <summary>Reuses one wire ID while an earlier response still belongs to its original caller.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public Task DelayedConsumerKeepsItsOwnReply(CancellationToken cancellationToken) =>
        AsyncBinaryCompletionTests.WithPeerAsync(VerifyDelayedConsumerAsync, cancellationToken);

    /// <summary>A timer observation from the previous use of a slot cannot cancel a later call.</summary>
    [Test]
    public async Task OldExpiryCannotCancelReusedCall()
    {
        var lifetime = new BinaryCallLifetime(10);
        var prior = lifetime.Expires;
        _ = await Assert.That(lifetime.TryComplete()).IsTrue();
        lifetime.Start(20);

        _ = await Assert.That(lifetime.TryExpire(prior)).IsFalse();
        _ = await Assert.That(lifetime.Expires).IsEqualTo(20L);
        _ = await Assert.That(lifetime.TryExpire(20)).IsTrue();
        _ = await Assert.That(lifetime.TryComplete()).IsFalse();
        lifetime.Start(0);
        _ = await Assert.That(lifetime.TryExpire(0)).IsFalse();
        _ = await Assert.That(lifetime.TryComplete()).IsTrue();
    }

    /// <summary>Expires one call while a later deadline keeps the same socket alive to consume its late reply.</summary>
    /// <param name="read">Whether the shorter attempt is a replayable read or a single-copy mutation.</param>
    /// <param name="cancelCaller">Whether caller cancellation wins before the attempt deadline.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    [Arguments(true, false)]
    [Arguments(true, true)]
    [Arguments(false, false)]
    [Arguments(false, true)]
    public Task ConnectionPreservesIndependentDeadlines(bool read, bool cancelCaller, CancellationToken cancellationToken) =>
        AsyncBinaryCompletionTests.WithPeerAsync(
            (client, peer, token) => read
                ? VerifyIndependentDeadlinesAsync(client, peer, AsyncBinaryCompletionTests.ReadLifetime, cancelCaller, token)
                : VerifyIndependentDeadlinesAsync(client, peer, AsyncBinaryCompletionTests.MutationLifetime, cancelCaller, token),
            cancellationToken);

    private static async Task CheckOversizedWriteAsync(BinaryCacheClient client, SslStream peer, CancellationToken token)
    {
        using var release = new ManualResetEventSlim();
        using var attempt = new CancellationTokenSource();
        using var bytes = new PausedPayload(release, token);
        var options = new BinaryCallOptions(DateTime.UtcNow.AddMinutes(1), attempt.Token, CancellationToken.None);
        var write = client.SetDistributedAsync(new SetDistributedAsyncRequest { CacheName = "default", Key = "oversized", Value = bytes.Payload }, in options).AsTask();
        try
        {
            await bytes.Entered.Task.WaitAsync(token);
            await attempt.CancelAsync();
            var error = await AsyncAssert.ThrowsAsync<SquirixTransportException, SetAsyncResponse>(new ValueTask<SetAsyncResponse>(write), TimeSpan.FromSeconds(5), TimeProvider.System, token);
            _ = await Assert.That(error.StatusCode).IsEqualTo(CacheTransportStatusCode.ResourceExhausted);
            _ = await Assert.That(error.Status.PayloadTooLarge).IsTrue();
        }
        finally
        {
            release.Set();
        }

        var read = AsyncBinaryCompletionTests.ReadLifetime;
        var survivor = read.Send(client, read.Request, new BinaryCallOptions(DateTime.UtcNow.AddSeconds(10), token, token));
        var id = await AsyncBinaryCompletionTests.ReadRequestAsync(peer, read.Code, read.Request, token);
        await AsyncBinaryCompletionTests.ReplyAsync(peer, id, read.Reply, token);
        _ = await Assert.That((await survivor.WaitAsync(token)).Found).IsTrue();
    }

    private static async Task VerifyDelayedConsumerAsync(BinaryCacheClient client, SslStream peer, CancellationToken token)
    {
        var read = AsyncBinaryCompletionTests.ReadLifetime;
        var options = new BinaryCallOptions(DateTime.UtcNow.AddSeconds(15), CancellationToken.None, CancellationToken.None);
        var first = client.GetDistributedAsync(read.Request, in options);
        var firstId = await AsyncBinaryCompletionTests.ReadRequestAsync(peer, read.Code, read.Request, token);
        var barrier = read.Send(client, read.Request, options);
        var barrierId = await AsyncBinaryCompletionTests.ReadRequestAsync(peer, read.Code, read.Request, token);
        await AsyncBinaryCompletionTests.ReplyAsync(peer, firstId, read.Reply, token);
        await AsyncBinaryCompletionTests.ReplyAsync(peer, barrierId, read.LateReply, token);
        _ = await barrier.WaitAsync(token);

        var next = read.Send(client, read.Request, options);
        var nextId = await AsyncBinaryCompletionTests.ReadRequestAsync(peer, read.Code, read.Request, token);
        _ = await Assert.That(nextId).IsEqualTo(firstId);
        await AsyncBinaryCompletionTests.ReplyAsync(peer, nextId, read.LateReply, token);
        var replacement = await next.WaitAsync(token);
        var original = await first;
        _ = await Assert.That(replacement.Found).IsFalse();
        _ = await Assert.That(original.Found).IsTrue();
        _ = await Assert.That(original.Value.Span.SequenceEqual(read.Reply.Value.Span)).IsTrue();
    }

    private static async Task VerifyIndependentDeadlinesAsync<TRequest, TResponse>(
        BinaryCacheClient client, SslStream peer, AsyncBinaryCompletionTests.LifetimeOperation<TRequest, TResponse> operation, bool cancelCaller, CancellationToken token)
        where TRequest : WireMessage
        where TResponse : WireMessage
    {
        var deadline = DateTime.UtcNow.AddSeconds(15);
        var survivorOptions = new BinaryCallOptions(deadline, default, default, BinaryCallAlarm.DueAfter(TimeSpan.FromSeconds(15)));
        var survivor = operation.Send(client, operation.Request, survivorOptions);
        var survivorId = await AsyncBinaryCompletionTests.ReadRequestAsync(peer, operation.Code, operation.Request, token);
        using var caller = new CancellationTokenSource();
        var bound = cancelCaller ? TimeSpan.FromSeconds(15) : TimeSpan.FromMilliseconds(250);
        var urgentOptions = new BinaryCallOptions(deadline, caller.Token, caller.Token, BinaryCallAlarm.DueAfter(bound));
        var urgent = operation.Send(client, operation.Request, urgentOptions);
        var urgentId = await AsyncBinaryCompletionTests.ReadRequestAsync(peer, operation.Code, operation.Request, token);
        if (cancelCaller)
            await caller.CancelAsync();
        if (!cancelCaller && operation.UnknownOnDeadline)
        {
            var error = await AsyncAssert.ThrowsAsync<SquirixTransportException, TResponse>(new ValueTask<TResponse>(urgent), TimeSpan.FromSeconds(5), TimeProvider.System, token);
            _ = await Assert.That(error.Status.Detail).IsEqualTo(CommitOutcomeUnknownException.StableDetail);
        }
        else
        {
            var error = await AsyncAssert.ThrowsAnyAsync<OperationCanceledException, TResponse>(new ValueTask<TResponse>(urgent), TimeSpan.FromSeconds(5), TimeProvider.System, token);
            _ = await Assert.That(error.CancellationToken.IsCancellationRequested).IsTrue();
            if (cancelCaller)
                _ = await Assert.That(error.CancellationToken).IsEqualTo(caller.Token);
        }

        _ = await Assert.That(survivor.IsCompleted).IsFalse();
        await AsyncBinaryCompletionTests.ReplyAsync(peer, urgentId, operation.LateReply, token);
        await AsyncBinaryCompletionTests.ReplyAsync(peer, survivorId, operation.Reply, token);
        var response = await survivor.WaitAsync(token);
        _ = await Assert.That(response.ToByteArray().AsSpan().SequenceEqual(operation.Reply.ToByteArray())).IsTrue();
    }

    private sealed class PausedPayload : MemoryManager<byte>
    {
        private readonly byte[] _bytes = new byte[Frames.MaxRequestBytes];
        private readonly ManualResetEventSlim _release;
        private readonly CancellationToken _token;

        internal PausedPayload(ManualResetEventSlim release, CancellationToken token)
        {
            _release = release;
            _token = token;
        }

        internal TaskCompletionSource Entered { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);

        internal ReadOnlyMemory<byte> Payload => CreateMemory(_bytes.Length);

        public override Span<byte> GetSpan()
        {
            _ = Entered.TrySetResult();
            _release.Wait(_token);
            return _bytes;
        }

        public override MemoryHandle Pin(int elementIndex = 0) => throw new NotSupportedException();

        public override void Unpin()
        {
        }

        protected override void Dispose(bool disposing)
        {
        }
    }
}
