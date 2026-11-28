using System;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Node.Services;
using Squirix.Server.Storage.Replication;
using Squirix.Server.TestKit;
using Squirix.Server.TestKit.Logging;
using Squirix.Server.UnitTests.Support;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;
using TUnit.Mocks;
using TUnit.Mocks.Arguments;
using TUnit.Mocks.Generated;
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

    private static Task<FollowerLogTail> AppendNoopAsync(ReplicaLeaderTenure tenure, IFollowerLog log, CancellationToken cancellationToken)
    {
        var factory = new ReplicaMutationFactory(new StubCache(), GroupId, Term, TimeProvider.System, NullLogger.Instance);
        var read = new FollowerLogTail(default, 0UL, []);
        return tenure.AppendNoopAsync(log, read, factory, GroupId, cancellationToken);
    }

    private static IFollowerLog NotReadyLog(bool disposed)
    {
        var log = IFollowerLog.Mock(MockBehavior.Strict);
        _ = log.AppendAsync(Arg.Any<FollowerLogAppendRequest>(), Arg.Any<CancellationToken>())
               .ReturnsAsync(Task.FromResult(new FollowerLogAppendResult(false, FollowerLogRefusal.NotReady, 0UL, 0UL)));
        _ = log.IsDisposed.Returns(disposed);
        return log.Object;
    }
}
