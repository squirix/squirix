using System;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging.Abstractions;
using Rocks;
using Squirix.Server.Cluster.Replication;
using Squirix.Server.Node.Services;
using Squirix.Server.Storage.Replication;
using Squirix.Server.TestKit;
using Squirix.Server.UnitTests.Support;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;
using static Squirix.Server.UnitTests.Node.Services.ReplicaOwnerTestKit;

namespace Squirix.Server.UnitTests.Node.Services;

/// <summary>The leader-term entry of a leadership is refused as shutdown only when the log was disposed before anything was scheduled.</summary>
public sealed class ReplicaLeaderTenureTests : ServerUnitTestBase
{
    private const string GroupId = "n1";
    private const ulong Term = 2UL;

    /// <summary>A drained dispose answers not ready: the leader-term entry is refused as shutdown and no entry is recorded.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task DisposedLogRefusesNoopAsShutdown(CancellationToken cancellationToken)
    {
        using var tenure = new ReplicaLeaderTenure(Term, new ReplicaDriverLease());
        var log = NotReadyLog(true);

        _ = await NodeAsyncAssert.ThrowsAsync<ReplicaShutdownRefusedException>(AppendNoopAsync(tenure, log, cancellationToken), TimeSpan.FromSeconds(30), TimeProvider.System, cancellationToken);

        _ = await Assert.That(tenure.NoopIndex).IsEqualTo(0UL);
    }

    /// <summary>A not-ready answer of a log that is not disposed stays a plain refusal.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task OpenLogRefusesNoopPlainly(CancellationToken cancellationToken)
    {
        using var tenure = new ReplicaLeaderTenure(Term, new ReplicaDriverLease());
        var log = NotReadyLog(false);

        _ = await NodeAsyncAssert.ThrowsAsync<InvalidOperationException>(AppendNoopAsync(tenure, log, cancellationToken), TimeSpan.FromSeconds(30), TimeProvider.System, cancellationToken);

        _ = await Assert.That(tenure.NoopIndex).IsEqualTo(0UL);
    }

    /// <summary>The tenure sends its immutable prepared no-op through the owned append path once.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task NoopUsesPreparedAppendOnce(CancellationToken cancellationToken)
    {
        using var tenure = new ReplicaLeaderTenure(Term, new ReplicaDriverLease());
        var log = new IFollowerLogCreateExpectations();
        var calls = 0;
        ReadOnlyMemory<byte> payload = default;
        _ = log.Setups.AppendPreparedAsync(Arg.Any<PreparedLogAppendRequest>(), Arg.Is<Action?>(null), Arg.Any<CancellationToken>())
               .Callback((request, _, _) =>
               {
                   calls++;
                   payload = request.Request.Entries.Span[0].Payload;
                   return Task.FromResult(new FollowerLogAppendResult(true, string.Empty, Term, 1));
               });
        _ = log.Setups.GetLeaderTailAsync(Arg.Any<CancellationToken>()).ReturnValue(ValueTask.FromResult(new FollowerLogTail(default, 1, [])));
        var instance = log.Instance();
        _ = await AppendNoopAsync(tenure, instance, cancellationToken);
        _ = await AppendNoopAsync(tenure, instance, cancellationToken);

        _ = await Assert.That(calls).IsEqualTo(1);
        _ = await Assert.That(tenure.NoopIndex).IsEqualTo(1UL);
        _ = await Assert.That(ReplicaLogCodec.IsWellFormed(payload.Span, 1, Term)).IsTrue();
    }

    private static Task<FollowerLogTail> AppendNoopAsync(ReplicaLeaderTenure tenure, IFollowerLog log, CancellationToken cancellationToken)
    {
        var factory = new ReplicaMutationFactory(new StubCache(), GroupId, Term, TimeProvider.System, NullLogger.Instance);
        var read = new FollowerLogTail(default, 0UL, []);
        return tenure.AppendNoopAsync(log, read, factory, GroupId, cancellationToken);
    }

    private static IFollowerLog NotReadyLog(bool disposed)
    {
        var log = new IFollowerLogCreateExpectations();
        _ = log.Setups.AppendPreparedAsync(Arg.Any<PreparedLogAppendRequest>(), Arg.Is<Action?>(null), Arg.Any<CancellationToken>())
               .ReturnValue(Task.FromResult(new FollowerLogAppendResult(false, FollowerLogRefusal.NotReady, 0UL, 0UL)));
        _ = log.Setups.IsDisposed.Gets().ReturnValue(disposed);
        return log.Instance();
    }
}
