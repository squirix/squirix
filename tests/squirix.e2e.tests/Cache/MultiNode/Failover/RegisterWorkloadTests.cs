using System;
using System.Threading;
using System.Threading.Tasks;
using Squirix.E2ETests.Fixtures;
using Squirix.Server.TestKit;
using Squirix.Transport;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;
using TUnit.Mocks;
using TUnit.Mocks.Arguments;
using TUnit.Mocks.Generated;

namespace Squirix.E2ETests.Cache.MultiNode.Failover;

/// <summary>The register workload records RPC and unknown-outcome failures as ambiguous writes or failed reads, and lets any other failure end it.</summary>
public sealed class RegisterWorkloadTests
{
    private static readonly string[] Keys = ["k"];

    /// <summary>
    /// A write that fails with an RPC status or an unknown commit outcome is recorded as ambiguous and the writer goes on; a read that fails
    /// with an RPC status is counted as failed.
    /// </summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task CallFailuresAreRecorded(CancellationToken cancellationToken)
    {
        var writer = ICache<long>.Mock(MockBehavior.Strict);
        _ = writer.SetAsync(Arg.Any<string>(), Arg.Any<long>(), Arg.Any<CacheEntryOptions?>(), Arg.Any<CancellationToken>())
                  .ReturnsAsync(static (_, value, _, _) => value switch
                  {
                      1L => Task.FromException(new SquirixTransportException(new CallStatus(CacheTransportStatusCode.Unavailable, "down"))),
                      2L => Task.FromException(new CommitOutcomeUnknownException("unknown")),
                      _ => Task.CompletedTask,
                  });
        var reads = 0;
        var reader = ICache<long>.Mock(MockBehavior.Strict);
        _ = reader.GetValueAsync(Arg.Any<string>(), Arg.Any<CancellationToken>())
                  .ReturnsAsync((_, _) => Interlocked.Increment(ref reads) == 1
                      ? Task.FromException<CacheValueResult<long>>(new SquirixTransportException(new CallStatus(CacheTransportStatusCode.DeadlineExceeded, "slow")))
                      : Task.FromResult(new CacheValueResult<long>(false, 0L)));
        var workload = new RegisterWorkload(writer.Object, reader.Object, Keys);

        await workload.RunAsync(3, 2, cancellationToken);

        _ = await Assert.That((workload.History.AmbiguousWrites, workload.History.FailedReads)).IsEqualTo((2, 1));
        _ = await Assert.That(workload.History.Summary()).IsEqualTo("1 acknowledged and 2 failed writes, 1 successful and 1 failed reads");
    }

    /// <summary>A write that fails with any other exception ends the workload with that exception.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task OtherWriteFailureEndsWorkload(CancellationToken cancellationToken)
    {
        var writer = ICache<long>.Mock(MockBehavior.Strict);
        _ = writer.SetAsync(Arg.Any<string>(), Arg.Any<long>(), Arg.Any<CacheEntryOptions?>(), Arg.Any<CancellationToken>())
                  .ReturnsAsync(static (_, _, _, _) => Task.FromException(new InvalidOperationException("bug")));
        var workload = new RegisterWorkload(writer.Object, ICache<long>.Mock(MockBehavior.Strict).Object, Keys);

        var failure = await NodeAsyncAssert.ThrowsAsync<InvalidOperationException>(workload.RunAsync(1, 0, cancellationToken), TimeSpan.FromSeconds(30), TimeProvider.System, cancellationToken);

        _ = await Assert.That(failure.Message).IsEqualTo("bug");
        _ = await Assert.That(workload.History.Summary()).IsEqualTo("0 acknowledged and 1 failed writes, 0 successful and 0 failed reads");
        _ = await Assert.That(workload.History.Snapshot().Writes[0].Acked).IsFalse();
    }

    /// <summary>A workload bounded by a task runs until the task completes, then the given number of operations per writer and reader.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task RunsUntilTaskThenTail(CancellationToken cancellationToken)
    {
        var until = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var writer = ICache<long>.Mock(MockBehavior.Strict);
        _ = writer.SetAsync(Arg.Any<string>(), Arg.Any<long>(), Arg.Any<CacheEntryOptions?>(), Arg.Any<CancellationToken>())
                  .ReturnsAsync((_, value, _, _) =>
                  {
                      if (value == 3L)
                          until.SetResult();

                      return Task.CompletedTask;
                  });
        var reader = ICache<long>.Mock(MockBehavior.Strict);
        _ = reader.GetValueAsync(Arg.Any<string>(), Arg.Any<CancellationToken>()).ReturnsAsync(Task.FromResult(new CacheValueResult<long>(false, 0L)));
        var workload = new RegisterWorkload(writer.Object, reader.Object, Keys);

        await workload.RunUntilAsync(until.Task, 2, cancellationToken);

        _ = await Assert.That(workload.History.Summary()).StartsWith("5 acknowledged and 0 failed writes", StringComparison.Ordinal);
    }

    /// <summary>A read that fails with any other exception ends the workload with that exception.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task OtherReadFailureEndsWorkload(CancellationToken cancellationToken)
    {
        var reader = ICache<long>.Mock(MockBehavior.Strict);
        _ = reader.GetValueAsync(Arg.Any<string>(), Arg.Any<CancellationToken>())
                  .ReturnsAsync(static (_, _) => Task.FromException<CacheValueResult<long>>(new InvalidOperationException("bug")));
        var workload = new RegisterWorkload(ICache<long>.Mock(MockBehavior.Strict).Object, reader.Object, Keys);

        var failure = await NodeAsyncAssert.ThrowsAsync<InvalidOperationException>(workload.RunAsync(0, 1, cancellationToken), TimeSpan.FromSeconds(30), TimeProvider.System, cancellationToken);

        _ = await Assert.That(failure.Message).IsEqualTo("bug");
        _ = await Assert.That(workload.History.FailedReads).IsEqualTo(1);
        _ = await Assert.That(workload.History.Snapshot().FailedCalls[0].Key).IsEqualTo(Keys[0]);
    }
}
