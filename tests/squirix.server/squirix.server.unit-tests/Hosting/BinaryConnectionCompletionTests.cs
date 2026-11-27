using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Adapters.Endpoint;
using Squirix.Transport.Binary;
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
