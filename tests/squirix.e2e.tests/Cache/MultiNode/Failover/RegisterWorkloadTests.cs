using System;
using System.Threading;
using System.Threading.Tasks;
using Grpc.Core;
using Rocks;
using Squirix.E2ETests.Fixtures;
using Squirix.Server.TestKit;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.E2ETests.Cache.MultiNode.Failover;

/// <summary>The register workload records RPC and unknown-outcome failures as ambiguous writes or failed reads, and lets any other failure end it.</summary>
public sealed class RegisterWorkloadTests : EndToEndTestBase
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
        var writer = new ICacheCreateExpectations<long>();
        _ = writer.Setups.SetAsync(Arg.Any<string>(), Arg.Any<long>(), Arg.Any<CacheEntryOptions?>(), Arg.Any<CancellationToken>())
                  .Callback(static (_, value, _, _) => value switch
                  {
                      1L => Task.FromException(new RpcException(new Status(StatusCode.Unavailable, "down"))),
                      2L => Task.FromException(new CommitOutcomeUnknownException("unknown")),
                      _ => Task.CompletedTask,
                  });
        var reads = 0;
        var reader = new ICacheCreateExpectations<long>();
        _ = reader.Setups.GetValueAsync(Arg.Any<string>(), Arg.Any<CancellationToken>())
                  .Callback((_, _) => Interlocked.Increment(ref reads) == 1
                      ? Task.FromException<CacheValueResult<long>>(new RpcException(new Status(StatusCode.DeadlineExceeded, "slow")))
                      : Task.FromResult(new CacheValueResult<long>(false, 0L)));
        var workload = new RegisterWorkload(writer.Instance(), reader.Instance(), Keys);

        await workload.RunAsync(3, 2, cancellationToken);

        _ = await Assert.That((workload.History.AmbiguousWrites, workload.History.FailedReads)).IsEqualTo((2, 1));
        _ = await Assert.That(workload.History.Summary()).IsEqualTo("1 acknowledged and 2 failed writes, 1 successful and 1 failed reads");
    }

    /// <summary>A write that fails with any other exception ends the workload with that exception.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task OtherWriteFailureEndsWorkload(CancellationToken cancellationToken)
    {
        var writer = new ICacheCreateExpectations<long>();
        _ = writer.Setups.SetAsync(Arg.Any<string>(), Arg.Any<long>(), Arg.Any<CacheEntryOptions?>(), Arg.Any<CancellationToken>())
                  .Callback(static (_, _, _, _) => Task.FromException(new InvalidOperationException("bug")));
        var workload = new RegisterWorkload(writer.Instance(), new ICacheCreateExpectations<long>().Instance(), Keys);

        var failure = await NodeAsyncAssert.ThrowsAsync<InvalidOperationException>(workload.RunAsync(1, 0, cancellationToken));

        _ = await Assert.That(failure.Message).IsEqualTo("bug");
        _ = await Assert.That(workload.History.Summary()).IsEqualTo("0 acknowledged and 0 failed writes, 0 successful and 0 failed reads");
    }

    /// <summary>A read that fails with any other exception ends the workload with that exception.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task OtherReadFailureEndsWorkload(CancellationToken cancellationToken)
    {
        var reader = new ICacheCreateExpectations<long>();
        _ = reader.Setups.GetValueAsync(Arg.Any<string>(), Arg.Any<CancellationToken>())
                  .Callback(static (_, _) => Task.FromException<CacheValueResult<long>>(new InvalidOperationException("bug")));
        var workload = new RegisterWorkload(new ICacheCreateExpectations<long>().Instance(), reader.Instance(), Keys);

        var failure = await NodeAsyncAssert.ThrowsAsync<InvalidOperationException>(workload.RunAsync(0, 1, cancellationToken));

        _ = await Assert.That(failure.Message).IsEqualTo("bug");
        _ = await Assert.That(workload.History.FailedReads).IsEqualTo(0);
    }
}
