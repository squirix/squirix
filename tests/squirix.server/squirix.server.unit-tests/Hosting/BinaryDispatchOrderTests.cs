using System;
using System.IO.Pipelines;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging.Abstractions;
using Squirix.Server.Adapters.Endpoint;
using Squirix.Server.Attributes;
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
            var replies = await output.ReadAtLeastAsync(4 * Frames.HeaderBytes, stop.Token);
            output.AdvanceTo(replies.Buffer.GetPosition(4 * Frames.HeaderBytes));
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

    private static async Task SendRequestsAsync(PipeWriter input, bool fragmented, CancellationToken cancellationToken)
    {
        const int split = 256 * 1024;
        var bodySize = fragmented ? split + 1 : 2;
        var bytes = GC.AllocateUninitializedArray<byte>((4 * Frames.HeaderBytes) + bodySize);
        var write = (Unsafe.BitCast<BinaryOperation, int>(BinaryOperation.SetDistributed) << 16) | 5000;
        var read = (Unsafe.BitCast<BinaryOperation, int>(BinaryOperation.GetDistributed) << 16) | 5000;
        Frames.Write(bytes, 1, bodySize, write);
        bytes.AsSpan(Frames.HeaderBytes, bodySize).Clear();
        Frames.Write(bytes.AsSpan(Frames.HeaderBytes + bodySize), 2, 0, write);
        Frames.Write(bytes.AsSpan((2 * Frames.HeaderBytes) + bodySize), 3, 0, write);
        Frames.Write(bytes.AsSpan((3 * Frames.HeaderBytes) + bodySize), 4, 0, read);

        if (fragmented)
            _ = await input.WriteAsync(bytes.AsMemory(0, split), cancellationToken).ConfigureAwait(false);
        _ = await input.WriteAsync(bytes.AsMemory(fragmented ? split : 0), cancellationToken).ConfigureAwait(false);
    }
}
