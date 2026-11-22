using System;
using System.Buffers;
using System.Collections.Generic;
using System.IO.Pipelines;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Adapters.Endpoint;
using Squirix.Server.Attributes;
using Squirix.Server.TestKit;
using Squirix.Server.TestKit.Logging;
using Squirix.Testing;
using Squirix.Transport.Binary;
using Squirix.Transport.Wire;
using Squirix.Transport.Wire.Cache;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Hosting;

/// <summary>Exercises bounded server transport reads against real in-memory pipes.</summary>
[Immutable]
public sealed class ConnectionPrefaceTests
{
    /// <summary>Incomplete framing is aborted at the handshake deadline before HTTP admission.</summary>
    [Test]
    public async Task IncompleteHeaderTimesOut()
    {
        var clock = new FakeTimeProvider();
        await using var connection = new ProbeConnection();
        _ = await connection.Incoming.Writer.WriteAsync(new ReadOnlyMemory<byte>([83, 81, 65, 85]), connection.ConnectionClosed);
        var admitted = false;
        var middleware = new ConnectionAuthenticationMiddleware(
            _ =>
            {
                admitted = true;
                return Task.CompletedTask;
            },
            null,
            clock);
        var processing = middleware.OnConnectionAsync(connection.Native);
        clock.Advance(TimeSpan.FromSeconds(5));
        await processing;
        _ = await Assert.That(connection.Aborted).IsTrue();
        _ = await Assert.That(admitted).IsFalse();
    }

    /// <summary>An incomplete binary request body closes the connection at the read deadline without dispatching the request.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task IncompleteBinaryRequestBodyTimesOut(CancellationToken cancellationToken)
    {
        var clock = new FakeTimeProvider();
        await using var connection = new ProbeConnection();
        using var stop = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        var frame = connection.Incoming.Writer.GetMemory(Frames.RequestBytes + 1);
        Frames.WriteRequest(frame.Span, 1, 2, 1);
        frame.Span[Frames.RequestBytes] = 0;
        connection.Incoming.Writer.Advance(Frames.RequestBytes + 1);
        _ = await connection.Incoming.Writer.FlushAsync(stop.Token);
        var dispatched = false;
        var pump = new BinaryConnectionPump(
            connection.Transport,
            connection.Abort,
            (_, _, _, _) =>
            {
                dispatched = true;
                return ValueTask.FromResult(default(BinaryResponse));
            },
            NullLogger.Instance,
            clock,
            connection.ConnectionClosed);
        var processing = pump.RunAsync(stop.Token);
        try
        {
            var tick = TimeSpan.FromMilliseconds(1);
            clock.Advance(RpcAttemptTimeout.OperationDeadline - tick);
            var pendingBeforeDeadline = !processing.IsCompleted && !connection.Aborted;

            clock.Advance(tick);
            await processing.WaitAsync(TimeSpan.FromSeconds(5), TimeProvider.System, cancellationToken);

            _ = await Assert.That(pendingBeforeDeadline).IsTrue();
            _ = await Assert.That(connection.Aborted).IsTrue();
            _ = await Assert.That(dispatched).IsFalse();
        }
        finally
        {
            await stop.CancelAsync();
            await processing;
        }
    }

    /// <summary>A ready reply is published before waiting for an incomplete following body.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task ReadyReplyPrecedesIncompleteBody(CancellationToken cancellationToken)
    {
        var clock = new FakeTimeProvider();
        await using var connection = new ProbeConnection();
        using var stop = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        stop.CancelAfter(TimeSpan.FromSeconds(10));
        var bytes = connection.Incoming.Writer.GetMemory((2 * Frames.RequestBytes) + 1);
        Frames.WriteRequest(bytes.Span, 1, 0, 1);
        Frames.WriteRequest(bytes.Span[Frames.RequestBytes..], 2, 2, 1);
        bytes.Span[2 * Frames.RequestBytes] = 0;
        connection.Incoming.Writer.Advance((2 * Frames.RequestBytes) + 1);
        _ = await connection.Incoming.Writer.FlushAsync(stop.Token);
        var dispatched = 0;
        var pump = new BinaryConnectionPump(
            connection.Transport,
            connection.Abort,
            (_, _, _, _) =>
            {
                dispatched++;
                return ValueTask.FromResult(default(BinaryResponse));
            },
            NullLogger.Instance,
            clock,
            connection.ConnectionClosed);
        var processing = pump.RunAsync(stop.Token);
        try
        {
            var result = await connection.Outgoing.Reader.ReadAtLeastAsync(Frames.ReplyBytes, stop.Token);
            var id = ReadReply(result.Buffer, 0).Id;
            connection.Outgoing.Reader.AdvanceTo(result.Buffer.GetPosition(Frames.ReplyBytes));
            _ = await Assert.That(id).IsEqualTo(1);
            _ = await Assert.That(dispatched).IsEqualTo(1);
            _ = await Assert.That(processing.IsCompleted).IsFalse();

            clock.Advance(RpcAttemptTimeout.OperationDeadline);
            await processing.WaitAsync(TimeSpan.FromSeconds(5), TimeProvider.System, cancellationToken);
            _ = await Assert.That(connection.Aborted).IsTrue();
            _ = await Assert.That(dispatched).IsEqualTo(1);
        }
        finally
        {
            await stop.CancelAsync();
            await processing;
        }
    }

    /// <summary>Ready replies share a flush while another request from the same input waits.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task ReadyRepliesBatchAroundSlowCall(CancellationToken cancellationToken)
    {
        await using var connection = new ProbeConnection();
        using var stop = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        stop.CancelAfter(TimeSpan.FromSeconds(10));
        var output = new FlushProbe(connection.Outgoing.Writer);
        connection.Transport = new TransportPipe(connection.Incoming.Reader, output);
        var delayed = new TaskCompletionSource<BinaryResponse>(TaskCreationOptions.RunContinuationsAsynchronously);
        var control = (System.Runtime.CompilerServices.Unsafe.BitCast<BinaryOperation, int>(BinaryOperation.GetDistributed) << 16) | 1;
        for (var id = 1; id <= 3; id++)
        {
            Frames.WriteRequest(connection.Incoming.Writer.GetSpan(Frames.RequestBytes), id, 0, control);
            connection.Incoming.Writer.Advance(Frames.RequestBytes);
        }

        _ = await connection.Incoming.Writer.FlushAsync(stop.Token);
        var pump = new BinaryConnectionPump(
            connection.Transport,
            connection.Abort,
            (frame, _, _, _) => frame.Id == 2 ? new ValueTask<BinaryResponse>(delayed.Task) : ValueTask.FromResult(default(BinaryResponse)),
            NullLogger.Instance,
            TimeProvider.System,
            connection.ConnectionClosed);
        var processing = pump.RunAsync(stop.Token);
        try
        {
            var ready = await connection.Outgoing.Reader.ReadAtLeastAsync(2 * Frames.ReplyBytes, stop.Token);
            var ids = (ReadReply(ready.Buffer, 0).Id, ReadReply(ready.Buffer, Frames.ReplyBytes).Id);
            connection.Outgoing.Reader.AdvanceTo(ready.Buffer.GetPosition(2 * Frames.ReplyBytes));
            _ = await Assert.That(ids).IsEqualTo((1, 3));
            _ = await Assert.That(output.Count).IsEqualTo(1);

            _ = delayed.TrySetResult(default);
            var later = await connection.Outgoing.Reader.ReadAtLeastAsync(Frames.ReplyBytes, stop.Token);
            var id = ReadReply(later.Buffer, 0).Id;
            connection.Outgoing.Reader.AdvanceTo(later.Buffer.GetPosition(Frames.ReplyBytes));
            _ = await Assert.That(id).IsEqualTo(2);
            _ = await Assert.That(output.Count).IsEqualTo(2);
        }
        finally
        {
            _ = delayed.TrySetResult(default);
            await stop.CancelAsync();
            await processing;
        }
    }

    /// <summary>The writer finishes a retained batch even while the input is idle.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task ReadyBatchSpansTwoFlushes(CancellationToken cancellationToken)
    {
        await using var connection = new ProbeConnection();
        using var stop = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        stop.CancelAfter(TimeSpan.FromSeconds(10));
        var output = new FlushProbe(connection.Outgoing.Writer);
        connection.Transport = new TransportPipe(connection.Incoming.Reader, output);
        for (var id = 1; id <= 2; id++)
        {
            Frames.WriteRequest(connection.Incoming.Writer.GetSpan(Frames.RequestBytes), id, 0, 1);
            connection.Incoming.Writer.Advance(Frames.RequestBytes);
        }

        _ = await connection.Incoming.Writer.FlushAsync(stop.Token);
        var large = new GetDistributedAsyncResponse { Found = true, Value = new byte[Frames.MaxBatchBytes] };
        var pump = new BinaryConnectionPump(
            connection.Transport,
            connection.Abort,
            (frame, _, _, _) => ValueTask.FromResult(frame.Id == 1 ? new BinaryResponse(FrameStatus.Ok, large) : default),
            NullLogger.Instance,
            TimeProvider.System,
            connection.ConnectionClosed);
        var processing = pump.RunAsync(stop.Token);
        try
        {
            var firstLength = Frames.ReplyBytes + large.CalculateSize();
            var totalLength = firstLength + Frames.ReplyBytes;
            var replies = await connection.Outgoing.Reader.ReadAtLeastAsync(totalLength, stop.Token);
            var ids = (ReadReply(replies.Buffer, 0).Id, ReadReply(replies.Buffer, firstLength).Id);
            connection.Outgoing.Reader.AdvanceTo(replies.Buffer.GetPosition(totalLength));

            _ = await Assert.That(ids).IsEqualTo((1, 2));
            _ = await Assert.That(output.Count).IsEqualTo(2);
        }
        finally
        {
            await stop.CancelAsync();
            await processing;
        }
    }

    /// <summary>A full batch may complete work before the pump handles input EOF.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task BinaryInputEofWaitsForAnOccupiedSlot(CancellationToken cancellationToken)
    {
        const int pendingCalls = 16;
        await using var connection = new ProbeConnection();
        using var stop = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        stop.CancelAfter(TimeSpan.FromSeconds(5));
        var input = connection.Incoming.Writer;
        for (var id = 1; id <= pendingCalls; id++)
        {
            Frames.WriteRequest(input.GetSpan(Frames.RequestBytes), id, 0, 1);
            input.Advance(Frames.RequestBytes);
        }

        _ = await input.FlushAsync(stop.Token);
        await input.CompleteAsync();
        var reply = new TaskCompletionSource<BinaryResponse>(TaskCreationOptions.RunContinuationsAsynchronously);
        var dispatched = 0;
        var allDispatched = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var pump = new BinaryConnectionPump(
            connection.Transport,
            connection.Abort,
            (_, _, _, _) =>
            {
                if (Interlocked.Increment(ref dispatched) == pendingCalls)
                    allDispatched.SetResult();
                return new ValueTask<BinaryResponse>(reply.Task);
            },
            NullLogger.Instance,
            TimeProvider.System,
            connection.ConnectionClosed);
        var processing = pump.RunAsync(stop.Token);
        try
        {
            await allDispatched.Task.WaitAsync(stop.Token);
            _ = await Assert.That(dispatched).IsEqualTo(pendingCalls);
            _ = await Assert.That(connection.Aborted).IsFalse();
        }
        finally
        {
            reply.SetResult(default);
            await processing.WaitAsync(stop.Token);
        }
    }

    /// <summary>Already queued Get calls drain after the endpoint stops accepting new requests.</summary>
    /// <param name="requests">The number of queued Get requests.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    [Arguments(3)]
    [Arguments(4)]
    public async Task QueuedBinaryGetsDrainOnGracefulShutdown(int requests, CancellationToken cancellationToken)
    {
        var control = (System.Runtime.CompilerServices.Unsafe.BitCast<BinaryOperation, int>(BinaryOperation.GetDistributed) << 16) | 1;
        await using var connection = new ProbeConnection();
        using var stop = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        using var guard = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        guard.CancelAfter(TimeSpan.FromSeconds(5));
        var input = connection.Incoming.Writer;
        for (var id = 1; id <= requests; id++)
        {
            Frames.WriteRequest(input.GetSpan(Frames.RequestBytes), id, 0, control);
            input.Advance(Frames.RequestBytes);
        }

        _ = await input.FlushAsync(guard.Token);
        var pump = new BinaryConnectionPump(
            connection.Transport,
            connection.Abort,
            async (frame, _, _, _) =>
            {
                if (frame.Id == 1)
                    await stop.CancelAsync().ConfigureAwait(false);

                return new BinaryResponse(FrameStatus.Ok);
            },
            NullLogger.Instance,
            TimeProvider.System,
            connection.ConnectionClosed);
        var processing = pump.RunAsync(stop.Token);
        try
        {
            var output = connection.Outgoing.Reader;
            var totalSize = requests * Frames.ReplyBytes;
            var result = await output.ReadAtLeastAsync(totalSize, guard.Token);
            var ok = System.Runtime.CompilerServices.Unsafe.BitCast<FrameStatus, int>(FrameStatus.Ok);
            var successfulIds = new HashSet<int>();
            for (var i = 0; i < requests; i++)
            {
                var (id, size, status) = ReadReply(result.Buffer, i * Frames.ReplyBytes);
                if (size == 0 && status == ok && id > 0 && id <= requests)
                    _ = successfulIds.Add(id);
            }

            output.AdvanceTo(result.Buffer.GetPosition(totalSize));
            await processing.WaitAsync(guard.Token);
            _ = await Assert.That(successfulIds.Count).IsEqualTo(requests);
            _ = await Assert.That(connection.Aborted).IsFalse();
        }
        finally
        {
            await stop.CancelAsync();
            await processing.WaitAsync(TimeSpan.FromSeconds(5), TimeProvider.System, CancellationToken.None);
        }
    }

    /// <summary>A split header resumes when exactly its remaining bytes arrive.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task SplitBinaryHeaderResumesAfterFirstReply(CancellationToken cancellationToken)
    {
        await using var connection = new ProbeConnection();
        using var stop = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        stop.CancelAfter(TimeSpan.FromSeconds(5));
        var pump = new BinaryConnectionPump(
            connection.Transport,
            connection.Abort,
            static (_, _, _, _) => ValueTask.FromResult(default(BinaryResponse)),
            NullLogger.Instance,
            TimeProvider.System,
            connection.ConnectionClosed);
        var processing = pump.RunAsync(stop.Token);
        try
        {
            const int split = Frames.RequestBytes / 2;
            var requests = GC.AllocateUninitializedArray<byte>(2 * Frames.RequestBytes);
            Frames.WriteRequest(requests, 1, 0, 1);
            Frames.WriteRequest(requests.AsSpan(Frames.RequestBytes), 2, 0, 1);
            var input = connection.Incoming.Writer;
            var output = connection.Outgoing.Reader;
            _ = await input.WriteAsync(requests.AsMemory(0, Frames.RequestBytes + split), stop.Token);
            var first = await output.ReadAtLeastAsync(Frames.ReplyBytes, stop.Token);
            var firstId = ReadReply(first.Buffer, 0).Id;
            output.AdvanceTo(first.Buffer.GetPosition(Frames.ReplyBytes));
            _ = await Assert.That(firstId).IsEqualTo(1);

            _ = await input.WriteAsync(requests.AsMemory(Frames.RequestBytes + split), stop.Token);
            var second = await output.ReadAtLeastAsync(Frames.ReplyBytes, stop.Token);
            var secondId = ReadReply(second.Buffer, 0).Id;
            output.AdvanceTo(second.Buffer.GetPosition(Frames.ReplyBytes));
            _ = await Assert.That(secondId).IsEqualTo(2);
        }
        finally
        {
            await stop.CancelAsync();
            await processing;
        }
    }

    /// <summary>A body larger than the pipe window is consumed incrementally, preserving the next frame.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task LargeBinaryBodyPreservesNextFrame(CancellationToken cancellationToken)
    {
        const int bodySize = 96 * 1024;
        await using var connection = new ProbeConnection();
        using var stop = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        stop.CancelAfter(TimeSpan.FromSeconds(10));
        var input = connection.Incoming.Writer;
        const int remainderLength = bodySize - 1;
        var expectedBody = new byte[bodySize];
        for (var offset = 0; offset < expectedBody.Length; offset++)
            expectedBody[offset] = byte.CreateChecked(offset % 251);
        var data = new byte[remainderLength + Frames.RequestBytes];
        expectedBody.AsSpan(1).CopyTo(data);
        Frames.WriteRequest(data.AsSpan(remainderLength), 3, 0, 1);
        var bodyIntact = false;
        var pump = new BinaryConnectionPump(
            connection.Transport,
            connection.Abort,
            (frame, body, _, _) =>
            {
                if (frame.Id == 2)
                    bodyIntact = body.Span.SequenceEqual(expectedBody);

                return ValueTask.FromResult(default(BinaryResponse));
            },
            NullLogger.Instance,
            TimeProvider.System,
            connection.ConnectionClosed);
        var processing = pump.RunAsync(stop.Token);
        try
        {
            const int prefixSize = (2 * Frames.RequestBytes) + 1;
            var prefix = input.GetMemory(prefixSize);
            Frames.WriteRequest(prefix.Span, 1, 0, 1);
            Frames.WriteRequest(prefix.Span[Frames.RequestBytes..], 2, bodySize, 1);
            prefix.Span[prefixSize - 1] = expectedBody[0];
            input.Advance(prefixSize);
            _ = await input.FlushAsync(stop.Token);

            var first = await connection.Outgoing.Reader.ReadAtLeastAsync(Frames.ReplyBytes, stop.Token);
            var firstId = ReadReply(first.Buffer, 0).Id;
            connection.Outgoing.Reader.AdvanceTo(first.Buffer.GetPosition(Frames.ReplyBytes));
            _ = await Assert.That(firstId).IsEqualTo(1);

            const int chunkSize = 8 * 1024;
            for (var offset = 0; offset < data.Length; offset += chunkSize)
                _ = await input.WriteAsync(data.AsMemory(offset, Math.Min(chunkSize, data.Length - offset)), stop.Token);

            var replies = await connection.Outgoing.Reader.ReadAtLeastAsync(2 * Frames.ReplyBytes, stop.Token);
            var ids = (ReadReply(replies.Buffer, 0).Id, ReadReply(replies.Buffer, Frames.ReplyBytes).Id);
            connection.Outgoing.Reader.AdvanceTo(replies.Buffer.GetPosition(2 * Frames.ReplyBytes));
            _ = await Assert.That(new HashSet<int> { ids.Item1, ids.Item2 }.SetEquals([2, 3])).IsTrue();
            _ = await Assert.That(bodyIntact).IsTrue();
        }
        finally
        {
            await stop.CancelAsync();
            await processing;
        }
    }

    /// <summary>Invalid size and unsupported version produce a strict negative acknowledgement.</summary>
    /// <param name="unsupportedVersion">Whether the peer uses an unsupported version instead of an oversized payload.</param>
    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task InvalidHeaderReceivesAcknowledgement(bool unsupportedVersion)
    {
        await using var connection = new ProbeConnection();
        var header = GC.AllocateUninitializedArray<byte>(8);
        ConnectionAuthenticationProtocol.WriteRequestHeader(header, 1);
        if (unsupportedVersion)
            header[4] = 2;
        else
            header[6] = 255;
        _ = await connection.Incoming.Writer.WriteAsync(header, connection.ConnectionClosed);
        var admitted = false;
        var middleware = new ConnectionAuthenticationMiddleware(
            _ =>
            {
                admitted = true;
                return Task.CompletedTask;
            },
            null,
            TimeProvider.System);
        await middleware.OnConnectionAsync(connection.Native);
        var status = await ReadStatusAsync(connection);
        _ = await Assert.That(status).IsEqualTo(unsupportedVersion ? ConnectionAuthenticationProtocol.UnsupportedVersion : ConnectionAuthenticationProtocol.InvalidCredentials);
        _ = await Assert.That(admitted).IsFalse();
    }

    /// <summary>Malformed UTF-8 is an explicit credential denial rather than an ambiguous transport failure.</summary>
    [Test]
    public async Task InvalidUtf8ReceivesCredentialDenial()
    {
        await using var provider = new TestServiceCollection().BuildServiceProvider();
        var options = new JwtValidationOptions();
        using var validator = new NativeConnectionAuthenticationValidator(() => options, TimeProvider.System, provider);
        await using var connection = new ProbeConnection();
        var header = GC.AllocateUninitializedArray<byte>(8);
        ConnectionAuthenticationProtocol.WriteRequestHeader(header, 1);
        _ = await connection.Incoming.Writer.WriteAsync(header, connection.ConnectionClosed);
        _ = await connection.Incoming.Writer.WriteAsync(new ReadOnlyMemory<byte>([255]), connection.ConnectionClosed);
        var admitted = false;
        var middleware = new ConnectionAuthenticationMiddleware(
            _ =>
            {
                admitted = true;
                return Task.CompletedTask;
            },
            validator,
            TimeProvider.System);
        await middleware.OnConnectionAsync(connection.Native);
        _ = await Assert.That(await ReadStatusAsync(connection)).IsEqualTo(ConnectionAuthenticationProtocol.InvalidCredentials);
        _ = await Assert.That(admitted).IsFalse();
        _ = await Assert.That(connection.Aborted).IsFalse();
        var available = HoldPermits(validator, 257);
        ReleasePermits(validator, available);
        _ = await Assert.That(available).IsEqualTo(256);
    }

    /// <summary>A peer waiting for its token body cannot occupy the final validation slot.</summary>
    [Test]
    public async Task StalledBodyLeavesValidationAvailable()
    {
        await using var provider = new TestServiceCollection().BuildServiceProvider();
        var options = new JwtValidationOptions();
        var clock = new FakeTimeProvider();
        using var validator = new NativeConnectionAuthenticationValidator(() => options, clock, provider);
        await using var connection = new ProbeConnection();
        var header = GC.AllocateUninitializedArray<byte>(8);
        ConnectionAuthenticationProtocol.WriteRequestHeader(header, 1);
        _ = await connection.Incoming.Writer.WriteAsync(header, connection.ConnectionClosed);
        var held = HoldPermits(validator, 255);
        var middleware = new ConnectionAuthenticationMiddleware(static _ => Task.CompletedTask, validator, clock);
        var processing = middleware.OnConnectionAsync(connection.Native);
        var available = validator.TryEnter();
        if (available)
            validator.Exit();
        clock.Advance(TimeSpan.FromSeconds(5));
        await processing;
        ReleasePermits(validator, held);
        _ = await Assert.That(held).IsEqualTo(255);
        _ = await Assert.That(available).IsTrue();
        _ = await Assert.That(connection.Aborted).IsTrue();
        var restored = HoldPermits(validator, 257);
        ReleasePermits(validator, restored);
        _ = await Assert.That(restored).IsEqualTo(256);
    }

    /// <summary>A complete token is consumed before capacity rejection, without entering the JWT validator.</summary>
    [Test]
    public async Task BusyConsumesCompletePayload()
    {
        await using var provider = new TestServiceCollection().BuildServiceProvider();
        var options = new JwtValidationOptions();
        using var validator = new NativeConnectionAuthenticationValidator(() => options, TimeProvider.System, provider);
        await using var connection = new ProbeConnection();
        var header = GC.AllocateUninitializedArray<byte>(8);
        ConnectionAuthenticationProtocol.WriteRequestHeader(header, 1);
        _ = await connection.Incoming.Writer.WriteAsync(header, connection.ConnectionClosed);
        _ = await connection.Incoming.Writer.WriteAsync(new ReadOnlyMemory<byte>([120]), connection.ConnectionClosed);
        var held = HoldPermits(validator, 256);
        var admitted = false;
        var middleware = new ConnectionAuthenticationMiddleware(
            _ =>
            {
                admitted = true;
                return Task.CompletedTask;
            },
            validator,
            TimeProvider.System);
        try
        {
            await middleware.OnConnectionAsync(connection.Native);
        }
        finally
        {
            ReleasePermits(validator, held);
        }

        _ = await Assert.That(held).IsEqualTo(256);
        _ = await Assert.That(await ReadStatusAsync(connection)).IsEqualTo(ConnectionAuthenticationProtocol.Busy);
        _ = await Assert.That(admitted).IsFalse();
        var unread = 0L;
        if (connection.Incoming.Reader.TryRead(out var remaining))
        {
            unread = remaining.Buffer.Length;
            connection.Incoming.Reader.AdvanceTo(remaining.Buffer.End);
        }

        _ = await Assert.That(unread).IsEqualTo(0);
    }

    /// <summary>Ordinary HTTP/2 retains the complete original preface for the next middleware without an authentication reply.</summary>
    [Test]
    public async Task OrdinaryHttp2PrefacePassesThrough()
    {
        await using var connection = new ProbeConnection();
        var preface = GC.AllocateUninitializedArray<byte>(24);
        "PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n"u8.CopyTo(preface);
        _ = await connection.Incoming.Writer.WriteAsync(preface, connection.ConnectionClosed);
        await connection.Incoming.Writer.CompleteAsync();
        var admitted = false;
        var originalBytesPreserved = false;
        var middleware = new ConnectionAuthenticationMiddleware(
            async context =>
            {
                admitted = true;
                var result = await context.Transport.Input.ReadAsync(context.ConnectionClosed);
                var buffer = result.Buffer;
                originalBytesPreserved = MatchesHttp2Preface(in buffer);
                context.Transport.Input.AdvanceTo(buffer.End);
            },
            null,
            TimeProvider.System);
        await middleware.OnConnectionAsync(connection.Native);
        var replyBytes = 0L;
        if (connection.Outgoing.Reader.TryRead(out var reply))
        {
            replyBytes = reply.Buffer.Length;
            connection.Outgoing.Reader.AdvanceTo(reply.Buffer.End);
        }

        _ = await Assert.That(admitted).IsTrue();
        _ = await Assert.That(connection.Aborted).IsFalse();
        _ = await Assert.That(originalBytesPreserved).IsTrue();
        _ = await Assert.That(replyBytes).IsEqualTo(0);
    }

    private static int HoldPermits(NativeConnectionAuthenticationValidator validator, int maximum)
    {
        var held = 0;
        while (held < maximum && validator.TryEnter())
            held++;
        return held;
    }

    private static void ReleasePermits(NativeConnectionAuthenticationValidator validator, int held)
    {
        for (var i = 0; i < held; i++)
            validator.Exit();
    }

    private static bool MatchesHttp2Preface(in ReadOnlySequence<byte> buffer)
    {
        if (buffer.Length != 24)
            return false;
        Span<byte> actual = stackalloc byte[24];
        buffer.Slice(0, 24).CopyTo(actual);
        return actual.SequenceEqual("PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n"u8);
    }

    private static async Task<byte> ReadStatusAsync(ProbeConnection connection)
    {
        var result = await connection.Outgoing.Reader.ReadAtLeastAsync(8, connection.ConnectionClosed);
        var buffer = result.Buffer;
        var status = ParseStatus(in buffer);
        connection.Outgoing.Reader.AdvanceTo(result.Buffer.End);
        return status;
    }

    private static byte ParseStatus(in ReadOnlySequence<byte> buffer)
    {
        Span<byte> header = stackalloc byte[8];
        buffer.Slice(0, 8).CopyTo(header);
        return ConnectionAuthenticationProtocol.TryReadAcknowledgement(header, out var status) ? status : byte.MaxValue;
    }

    private static (int Id, int Size, int Status) ReadReply(in ReadOnlySequence<byte> buffer, int offset)
    {
        Span<byte> header = stackalloc byte[Frames.ReplyBytes];
        buffer.Slice(offset, header.Length).CopyTo(header);
        return Frames.ReadReply(header);
    }

    internal sealed class ProbeConnection : IAsyncDisposable
    {
        internal ProbeConnection()
        {
            Transport = new TransportPipe(Incoming.Reader, Outgoing.Writer);
            Native = new BinaryConnection("preface", Transport, null, null, CancellationToken.None, CancellationToken.None);
        }

        internal Pipe Incoming { get; } = new();

        internal Pipe Outgoing { get; } = new();

        internal BinaryConnection Native { get; }

        internal IDuplexPipe Transport { get; set; }

        internal CancellationToken ConnectionClosed => Native.ConnectionClosed;

        internal bool Aborted => Native.ConnectionClosed.IsCancellationRequested;

        public async ValueTask DisposeAsync()
        {
            await Incoming.Writer.CompleteAsync();
            await Incoming.Reader.CompleteAsync();
            await Outgoing.Writer.CompleteAsync();
            await Outgoing.Reader.CompleteAsync();
            Native.Dispose();
        }

        internal void Abort() => Native.Abort();
    }

    private sealed class FlushProbe : PipeWriter
    {
        private readonly PipeWriter _inner;
        private int _count;

        internal FlushProbe(PipeWriter inner)
        {
            _inner = inner;
        }

        internal int Count => Volatile.Read(ref _count);

        public override void Advance(int bytes) => _inner.Advance(bytes);

        public override Memory<byte> GetMemory(int sizeHint = 0) => _inner.GetMemory(sizeHint);

        public override Span<byte> GetSpan(int sizeHint = 0) => _inner.GetSpan(sizeHint);

        public override void CancelPendingFlush() => _inner.CancelPendingFlush();

        public override void Complete(Exception? exception = null) => _inner.Complete(exception);

        public override ValueTask<FlushResult> FlushAsync(CancellationToken cancellationToken = default)
        {
            _ = Interlocked.Increment(ref _count);
            return _inner.FlushAsync(cancellationToken);
        }
    }

    private sealed class TransportPipe : IDuplexPipe
    {
        internal TransportPipe(PipeReader input, PipeWriter output)
        {
            Input = input;
            Output = output;
        }

        public PipeReader Input { get; }

        public PipeWriter Output { get; }
    }
}
