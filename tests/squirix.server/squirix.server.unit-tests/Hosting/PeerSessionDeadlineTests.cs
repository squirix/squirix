using System;
using System.IO.Pipelines;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Time.Testing;
using Squirix.Server.Adapters.Endpoint;
using Squirix.Server.Cluster.Transport;
using Squirix.Server.TestKit;
using Squirix.Transport.Binary;
using Squirix.Transport.Wire;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Hosting;

/// <summary>Checks cancellation of peer I/O independently of application dispatch.</summary>
public sealed class PeerSessionDeadlineTests
{
    /// <summary>A peer session waiting for a new frame releases the read when its idle time elapses.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task IdleHeaderReadExpires(CancellationToken cancellationToken)
    {
        var clock = new FakeTimeProvider();
        var pipe = new Pipe();
        await using var input = pipe.Reader.AsStream();
        var pending = NativePeerEndpoint.ReadHeaderAsync(input, new byte[PeerFrames.RequestBytes], clock, cancellationToken);
        _ = await Assert.That(pending.IsCompleted).IsFalse();
        clock.Advance(PeerFrames.IdleTimeout);
        _ = await NodeAsyncAssert.ThrowsAnyAsync<OperationCanceledException>(pending, TimeSpan.FromSeconds(5), TimeProvider.System, cancellationToken);
        await pipe.Writer.CompleteAsync();
    }

    /// <summary>Reply cleanup expires independently even when the listener is still serving other sessions.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task BlockedReplyWriteExpires(CancellationToken cancellationToken)
    {
        var clock = new FakeTimeProvider();
        var pipe = new Pipe(new PipeOptions(pauseWriterThreshold: 1, resumeWriterThreshold: 1));
        await using var output = pipe.Writer.AsStream();
        var pending = NativePeerEndpoint.WriteAsync(output, new BinaryResponse(FrameStatus.DeadlineExceeded), clock, cancellationToken);
        _ = await Assert.That(pending.IsCompleted).IsFalse();
        clock.Advance(RpcAttemptTimeout.OperationDeadline);
        _ = await NodeAsyncAssert.ThrowsAnyAsync<OperationCanceledException>(pending, TimeSpan.FromSeconds(5), TimeProvider.System, cancellationToken);
        await pipe.Reader.CompleteAsync();
    }
}
