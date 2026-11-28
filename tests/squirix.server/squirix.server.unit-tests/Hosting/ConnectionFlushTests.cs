using System;
using System.IO.Pipelines;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Adapters.Endpoint;
using Squirix.Server.TestKit;
using Squirix.Testing;
using Squirix.Transport.Wire;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Hosting;

/// <summary>Exercises the shared response writer deadline with a backpressured pipe.</summary>
public sealed class ConnectionFlushTests
{
    /// <summary>The alarm left by a completed write gives the next flush its full time budget.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task CompletedFlushCannotExpireItsSuccessor(CancellationToken cancellationToken)
    {
        var clock = new FakeTimeProvider();
        using var bounded = new ConnectionFlush(clock, cancellationToken);
        var pipe = new Pipe(new PipeOptions(pauseWriterThreshold: 2, resumeWriterThreshold: 1, useSynchronizationContext: false));
        try
        {
            pipe.Writer.GetSpan(1)[0] = 1;
            pipe.Writer.Advance(1);
            var first = await bounded.WriteAsync(pipe.Writer);
            _ = await Assert.That(first.IsCanceled).IsFalse();
            clock.Advance(RpcAttemptTimeout.OperationDeadline - TimeSpan.FromSeconds(1));
            pipe.Writer.GetSpan(1)[0] = 2;
            pipe.Writer.Advance(1);
            var second = bounded.WriteAsync(pipe.Writer).AsTask();
            clock.Advance(TimeSpan.FromSeconds(1));
            _ = await Assert.That(second.IsCompleted).IsFalse();
            clock.Advance(RpcAttemptTimeout.OperationDeadline - TimeSpan.FromSeconds(1));
            _ = await NodeAsyncAssert.ThrowsAnyAsync<OperationCanceledException>(second, TimeSpan.FromSeconds(10), TimeProvider.System, cancellationToken);
        }
        finally
        {
            await pipe.Reader.CompleteAsync();
            await pipe.Writer.CompleteAsync();
        }
    }
}
