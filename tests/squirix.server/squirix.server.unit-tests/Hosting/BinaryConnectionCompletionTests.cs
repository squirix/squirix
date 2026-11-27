using System;
using System.IO;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Adapters.Endpoint;
using Squirix.Server.TestKit.Logging;
using Squirix.Transport.Binary;
using Squirix.Transport.Wire.Cache;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Hosting;

/// <summary>Verifies stream-backed output completion on the public transport.</summary>
public sealed class BinaryConnectionCompletionTests
{
    /// <summary>Graceful completion flushes pending output, while a connection abort discards it.</summary>
    /// <param name="abort">Whether the session ended with a forced abort.</param>
    /// <param name="expectedBytes">The number of bytes delivered during completion.</param>
    [Test]
    [Arguments(false, 1L)]
    [Arguments(true, 0L)]
    public async Task CompletionHonorsSessionOutcome(bool abort, long expectedBytes)
    {
        await using var destination = new MemoryStream();
        var pipe = new NativeBinaryEndpoint.StreamPipe(destination);
        using var connection = new BinaryConnection("completion", pipe, null, null, CancellationToken.None, CancellationToken.None);
        pipe.Output.GetSpan(1)[0] = 42;
        pipe.Output.Advance(1);
        if (abort)
            connection.Abort();
        else
            connection.RequestClose();

        await connection.CompleteTransportAsync();
        _ = await Assert.That(destination.Length).IsEqualTo(expectedBytes);
    }

    /// <summary>Flushing a ready batch writes its frames contiguously to the TLS stream.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task ReadyFramesShareOneStreamWrite(CancellationToken cancellationToken)
    {
        const int frameBytes = 833;
        await using var destination = new CountingStream();
        var pipe = new NativeBinaryEndpoint.StreamPipe(destination);
        using var connection = new BinaryConnection("batch", pipe, null, null, cancellationToken, cancellationToken);
        for (var i = 0; i < Frames.MaxBatch; i++)
        {
            pipe.Output.GetSpan(frameBytes)[..frameBytes].Fill(byte.CreateChecked(i));
            pipe.Output.Advance(frameBytes);
        }

        _ = await pipe.Output.FlushAsync(cancellationToken);
        await connection.CompleteTransportAsync();
        _ = await Assert.That(destination.Writes).IsEqualTo(1);
        _ = await Assert.That(destination.Length).IsEqualTo(long.CreateChecked(Frames.MaxBatch * frameBytes));
        var bytes = destination.ToArray();
        for (var i = 0; i < Frames.MaxBatch; i++)
        {
            var intact = bytes.AsSpan(i * frameBytes, frameBytes).IndexOfAnyExcept(byte.CreateChecked(i)) < 0;
            _ = await Assert.That(intact).IsTrue();
        }
    }

    /// <summary>The stream reader releases its page when the last dispatched call finishes.</summary>
    /// <param name="faulted">Whether the handler throws after using the borrowed body.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    [NotInParallel]
    [Arguments(false)]
    [Arguments(true)]
    public async Task StreamReadPageIsReleasedAfterHandler(bool faulted, CancellationToken cancellationToken)
    {
        const byte marker = 42;
        const int requestBytes = Frames.RequestBytes + 1;
        await using var stream = new MemoryStream(requestBytes + Frames.ReplyBytes);
        stream.SetLength(requestBytes);
        var request = stream.GetBuffer();
        var control = (Unsafe.BitCast<BinaryOperation, int>(BinaryOperation.SetDistributed) << 16) | 5000;
        Frames.WriteRequest(request, 1, 1, control);
        request[requestBytes - 1] = marker;

        var pipe = new NativeBinaryEndpoint.StreamPipe(stream);
        using var connection = new BinaryConnection("read-page", pipe, null, null, CancellationToken.None, CancellationToken.None);
        using var stopReading = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        var completeCall = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var received = byte.MinValue;

        var read = await pipe.Input.ReadAtLeastAsync(requestBytes, cancellationToken);
        var (page, _) = BinaryReadPage.Find(read.Buffer.First);
        pipe.Input.AdvanceTo(read.Buffer.Start);
        _ = await Assert.That(page).IsNotNull();

        var pump = new BinaryConnectionPump(
            pipe,
            connection.Abort,
            async (_, body, _, _) =>
            {
                // Stop after this frame so a later read cannot rent the page again.
                await stopReading.CancelAsync();
                await completeCall.Task.WaitAsync(cancellationToken);
                received = body.Span[0];
                return faulted ? throw new InvalidOperationException("Handler failed.") : new BinaryResponse(FrameStatus.Ok);
            },
            NullLogger.Instance,
            TimeProvider.System,
            connection.ConnectionClosed);

        var processing = pump.RunAsync(stopReading.Token);
        var heldBytes = page!.Bytes.Length;
        completeCall.SetResult();
        try
        {
            await processing.WaitAsync(cancellationToken);
            var releasedBytes = page.Bytes.Length;
            var (id, _, status) = Frames.ReadReply(stream.ToArray().AsSpan(requestBytes));
            _ = await Assert.That(id).IsEqualTo(1);
            _ = await Assert.That(status).IsEqualTo(Unsafe.BitCast<FrameStatus, int>(faulted ? FrameStatus.Unknown : FrameStatus.Ok));
            _ = await Assert.That(received).IsEqualTo(marker);
            _ = await Assert.That(heldBytes).IsGreaterThanOrEqualTo(requestBytes);
            _ = await Assert.That(releasedBytes).IsEqualTo(0);
        }
        finally
        {
            await connection.CompleteTransportAsync();
        }
    }

    private sealed class CountingStream : MemoryStream
    {
        internal int Writes { get; private set; }

        public override ValueTask WriteAsync(ReadOnlyMemory<byte> buffer, CancellationToken cancellationToken = default)
        {
            Writes++;
            return base.WriteAsync(buffer, cancellationToken);
        }
    }
}
