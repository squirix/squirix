using System;
using System.IO.Pipelines;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Adapters.Endpoint;
using Squirix.Server.Attributes;
using Squirix.Server.TestKit.Logging;
using Squirix.Transport.Binary;
using Squirix.Transport.Wire.Cache;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Hosting;

/// <summary>Checks ordering across queued binary transport work.</summary>
[Immutable]
public sealed class BinaryDispatchOrderTests
{
    /// <summary>Prepared mutations and reads wait for preceding writes even across a rejected request.</summary>
    /// <param name="fragmented">Whether the first request uses a large body split across pipe reads.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task PreparedCallsPreserveWriteOrder(bool fragmented, CancellationToken cancellationToken)
    {
        await using var connection = new ConnectionPrefaceTests.ProbeConnection();
        using var stop = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        stop.CancelAfter(TimeSpan.FromSeconds(10));
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var prepared = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var observedValue = 0;
        var value = 0;
        var pump = new BinaryConnectionPump(
            connection.Transport,
            connection.Abort,
            async (frame, _, _, predecessor) =>
            {
                if (frame.Id == 1)
                    await release.Task.WaitAsync(stop.Token).ConfigureAwait(false);
                if (frame.Id == 2)
                    return new BinaryResponse(FrameStatus.InvalidArgument);
                if (frame.Id == 4)
                    prepared.SetResult();
                await predecessor.WaitAsync(stop.Token).ConfigureAwait(false);

                if (frame.Id == 4)
                    observedValue = value;
                else
                    value = frame.Id;
                return new BinaryResponse(FrameStatus.Ok);
            },
            NullLogger.Instance,
            TimeProvider.System,
            connection.ConnectionClosed);
        var processing = pump.RunAsync(stop.Token);
        try
        {
            await SendRequestsAsync(connection.Incoming.Writer, fragmented, stop.Token);

            await prepared.Task.WaitAsync(stop.Token);
            _ = await Assert.That(observedValue).IsEqualTo(0);
            release.SetResult();
            var output = connection.Outgoing.Reader;
            var replies = await output.ReadAtLeastAsync(4 * Frames.ReplyBytes, stop.Token);
            output.AdvanceTo(replies.Buffer.GetPosition(4 * Frames.ReplyBytes));
            _ = await Assert.That(observedValue).IsEqualTo(3);
            _ = await Assert.That(value).IsEqualTo(3);
        }
        finally
        {
            _ = release.TrySetResult();
            await stop.CancelAsync();
            await processing.WaitAsync(TimeSpan.FromSeconds(5), TimeProvider.System, CancellationToken.None);
        }
    }

    /// <summary>A suspended call retains its pipe bytes while the reader processes subsequent frames.</summary>
    /// <param name="ordered">Whether the first call starts inline on the reader.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task SuspendedCallKeepsInputPage(bool ordered, CancellationToken cancellationToken)
    {
        await using var connection = new ConnectionPrefaceTests.ProbeConnection(new PipeOptions(BinaryReadPage.Pool));
        using var stop = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        stop.CancelAfter(TimeSpan.FromSeconds(10));
        var entered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        byte[]? firstPage = null;
        var valid = false;
        var prefixValid = false;
        var pump = new BinaryConnectionPump(
            connection.Transport,
            connection.Abort,
            async (frame, body, _, _) =>
            {
                if (frame.Id == 0)
                {
                    await entered.Task.WaitAsync(stop.Token).ConfigureAwait(false);
                    prefixValid = MemoryMarshal.TryGetArray(body, out var prefix) && ReferenceEquals(prefix.Array, firstPage) &&
                                  body.Length == 1 && body.Span[0] == 42;
                }
                else if (frame.Id == 1)
                {
                    entered.SetResult();
                    await release.Task.WaitAsync(stop.Token).ConfigureAwait(false);
                    valid = MemoryMarshal.TryGetArray(body, out var segment) && ReferenceEquals(segment.Array, firstPage) &&
                            body.Span.IndexOfAnyExcept<byte>(1) < 0;
                }

                return new BinaryResponse(FrameStatus.Ok);
            },
            NullLogger.Instance,
            TimeProvider.System,
            connection.ConnectionClosed);

        var processing = pump.RunAsync(stop.Token);
        try
        {
            var writer = connection.Incoming.Writer;
            firstPage = WritePage(writer, 1, ordered ? BinaryOperation.SetDistributed : BinaryOperation.GetValue);
            _ = await writer.FlushAsync(stop.Token);
            await ReadReplyAsync(connection.Outgoing.Reader, 0, stop.Token);
            _ = await Assert.That(prefixValid).IsTrue();
            for (var id = 2; id <= 4; id++)
            {
                var nextPage = WritePage(writer, id, BinaryOperation.GetValue);
                _ = await Assert.That(ReferenceEquals(firstPage, nextPage)).IsFalse();
                _ = await writer.FlushAsync(stop.Token);
                await ReadReplyAsync(connection.Outgoing.Reader, id, stop.Token);
            }

            release.SetResult();
            await ReadReplyAsync(connection.Outgoing.Reader, 1, stop.Token);
            _ = await Assert.That(valid).IsTrue();
        }
        finally
        {
            _ = release.TrySetResult();
            await stop.CancelAsync();
            await processing.WaitAsync(TimeSpan.FromSeconds(5), TimeProvider.System, CancellationToken.None);
        }
    }

    private static byte[] WritePage(PipeWriter writer, int id, BinaryOperation operation)
    {
        var memory = writer.GetMemory(4096);
        var bytes = memory.Span;
        if (id == 1)
        {
            Frames.WriteRequest(bytes, 0, 1, (Unsafe.BitCast<BinaryOperation, int>(BinaryOperation.GetValue) << 16) | 5000);
            bytes[Frames.RequestBytes] = 42;
            bytes = bytes[(Frames.RequestBytes + 1)..];
        }

        Frames.WriteRequest(bytes, id, bytes.Length - Frames.RequestBytes, (Unsafe.BitCast<BinaryOperation, int>(operation) << 16) | 5000);
        bytes[Frames.RequestBytes..].Fill(byte.CreateChecked(id));
        var array = MemoryMarshal.TryGetArray<byte>(memory, out var segment) ? segment.Array! : throw new InvalidOperationException();
        writer.Advance(memory.Length);
        return array;
    }

    private static async Task ReadReplyAsync(PipeReader reader, int expectedId, CancellationToken cancellationToken)
    {
        var read = await reader.ReadAtLeastAsync(Frames.ReplyBytes, cancellationToken);
        var (id, _, status) = Frames.ReadReply(read.Buffer.FirstSpan);
        reader.AdvanceTo(read.Buffer.GetPosition(Frames.ReplyBytes));
        _ = await Assert.That(id).IsEqualTo(expectedId);
        _ = await Assert.That(status).IsEqualTo(0);
    }

    private static async Task SendRequestsAsync(PipeWriter input, bool fragmented, CancellationToken cancellationToken)
    {
        const int split = 256 * 1024;
        var bodySize = fragmented ? split + 1 : 2;
        var bytes = GC.AllocateUninitializedArray<byte>((4 * Frames.RequestBytes) + bodySize);
        var write = (Unsafe.BitCast<BinaryOperation, int>(BinaryOperation.SetDistributed) << 16) | 5000;
        var read = (Unsafe.BitCast<BinaryOperation, int>(BinaryOperation.GetDistributed) << 16) | 5000;
        Frames.WriteRequest(bytes, 1, bodySize, write);
        bytes.AsSpan(Frames.RequestBytes, bodySize).Clear();
        Frames.WriteRequest(bytes.AsSpan(Frames.RequestBytes + bodySize), 2, 0, write);
        Frames.WriteRequest(bytes.AsSpan((2 * Frames.RequestBytes) + bodySize), 3, 0, write);
        Frames.WriteRequest(bytes.AsSpan((3 * Frames.RequestBytes) + bodySize), 4, 0, read);

        if (fragmented)
            _ = await input.WriteAsync(bytes.AsMemory(0, split), cancellationToken).ConfigureAwait(false);
        _ = await input.WriteAsync(bytes.AsMemory(fragmented ? split : 0), cancellationToken).ConfigureAwait(false);
    }
}
