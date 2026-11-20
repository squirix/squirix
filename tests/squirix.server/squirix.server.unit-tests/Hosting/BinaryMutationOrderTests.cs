using System;
using System.Buffers;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO.Pipelines;
using System.Runtime.CompilerServices;
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

/// <summary>Exercises mutation ordering across batches on a single binary connection.</summary>
[Immutable]
public sealed class BinaryMutationOrderTests
{
    private const int ReadId = 7;
    private const int RejectedWrite = 3;

    /// <summary>A rejected write does not let later calls pass an earlier blocked mutation.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task BufferedWritesKeepOrder(CancellationToken cancellationToken)
    {
        await using var connection = new ConnectionPrefaceTests.ProbeConnection();
        using var stop = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        stop.CancelAfter(TimeSpan.FromSeconds(5));
        var unblock = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var getWaiting = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
        var mutations = new ConcurrentQueue<int>();
        await WriteRequestsAsync(connection.Incoming.Writer, stop.Token);
        var pump = new BinaryConnectionPump(
            connection.Transport,
            connection.Abort,
            async (frame, _, _, predecessor) =>
            {
                if (frame.Id == RejectedWrite)
                    return new BinaryResponse(FrameStatus.InvalidArgument);

                if (frame.Id == ReadId)
                    _ = getWaiting.TrySetResult(!predecessor.IsCompleted);
                await predecessor.WaitAsync(stop.Token).ConfigureAwait(false);

                if (frame.Id == 1)
                    await unblock.Task.WaitAsync(stop.Token).ConfigureAwait(false);
                if (frame.Id != ReadId)
                    mutations.Enqueue(frame.Id);

                return new BinaryResponse(FrameStatus.Ok);
            },
            NullLogger.Instance,
            TimeProvider.System,
            connection.ConnectionClosed);
        var processing = pump.RunAsync(stop.Token);
        try
        {
            _ = await Assert.That(await getWaiting.Task.WaitAsync(stop.Token)).IsTrue();
            _ = await Assert.That(mutations.IsEmpty).IsTrue();
            unblock.SetResult();

            await AssertRepliesAsync(connection.Outgoing.Reader, stop.Token);
            _ = await Assert.That(mutations.ToArray().AsSpan().SequenceEqual<int>([1, 2, 4, 5, 6])).IsTrue();
        }
        finally
        {
            _ = unblock.TrySetResult();
            await stop.CancelAsync();
            await processing.WaitAsync(TimeSpan.FromSeconds(5), TimeProvider.System, CancellationToken.None);
        }
    }

    private static async ValueTask WriteRequestsAsync(PipeWriter input, CancellationToken stop)
    {
        for (var id = 1; id <= ReadId; id++)
        {
            var operation = id % 2 == 0 ? BinaryOperation.Remove : BinaryOperation.SetDistributed;
            if (id == ReadId)
                operation = BinaryOperation.GetDistributed;

            var control = (Unsafe.BitCast<BinaryOperation, int>(operation) << 16) | 1000;
            Frames.Write(input.GetSpan(Frames.HeaderBytes), id, 0, control);
            input.Advance(Frames.HeaderBytes);
        }

        _ = await input.FlushAsync(stop);
    }

    private static async Task AssertRepliesAsync(PipeReader output, CancellationToken stop)
    {
        const int byteCount = ReadId * Frames.HeaderBytes;
        var result = await output.ReadAtLeastAsync(byteCount, stop);
        var buffer = result.Buffer;
        var replies = ReadReplies(in buffer);
        output.AdvanceTo(result.Buffer.GetPosition(byteCount));
        for (var id = 1; id <= ReadId; id++)
        {
            var expected = id == RejectedWrite ? FrameStatus.InvalidArgument : FrameStatus.Ok;
            _ = await Assert.That(replies[id]).IsEqualTo((0, Unsafe.BitCast<FrameStatus, int>(expected)));
        }
    }

    private static Dictionary<int, (int Size, int Status)> ReadReplies(in ReadOnlySequence<byte> buffer)
    {
        var replies = new Dictionary<int, (int Size, int Status)>();
        Span<byte> header = stackalloc byte[Frames.HeaderBytes];
        for (var i = 0; i < ReadId; i++)
        {
            buffer.Slice(i * Frames.HeaderBytes, Frames.HeaderBytes).CopyTo(header);
            var (id, size, status) = Frames.Read(header, Frames.MaxResponseBytes);
            replies.Add(id, (size, status));
        }

        return replies;
    }
}
