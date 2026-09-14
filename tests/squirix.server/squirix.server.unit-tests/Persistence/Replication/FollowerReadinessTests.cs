using System;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Attributes;
using Squirix.Server.Storage.Replication;
using Squirix.Server.TestKit.IO;
using Squirix.Server.UnitTests.Support;
using Xunit;

namespace Squirix.Server.UnitTests.Persistence.Replication;

/// <summary>Visibility of follower-log readiness to lock-free external pollers (issue #450 S5).</summary>
[Immutable]
public sealed class FollowerReadinessTests : ServerUnitTestBase
{
    private const string GroupId = "grp-1";

    /// <summary>Concurrent readiness polls racing startup observe only defined readiness states and settle on ready.</summary>
    [Fact]
    public async Task ConcurrentPollsObserveDefinedReadiness()
    {
        using var dir = new TempDirectory("squirix-follower-readiness-visibility");
        var composition = GroupComposition.Create(GroupId);
        await using var log = new FollowerLog(dir, GroupId, composition);
        var cancellationToken = DefaultCancellationToken;

        // A start gate releases every poller at once so readiness reads race the gated startup writes.
        using var gate = new ManualResetEventSlim(false);
        var polls = StartReadinessPollers(log, gate, 4, cancellationToken);

        gate.Set();
        await log.OpenAsync(cancellationToken);

        var undefined = 0;
        for (var i = 0; i < polls.Length; i++)
            undefined += await polls[i].WaitAsync(TimeSpan.FromSeconds(30), TimeProvider.System, cancellationToken);

        Assert.Equal(0, undefined);
        Assert.Equal(FollowerLogReadiness.Ready, log.Readiness);
        Assert.Equal(FollowerLogReadiness.Ready, (await log.GetStatusAsync(cancellationToken)).Readiness);
    }

    private static Task<int>[] StartReadinessPollers(FollowerLog log, ManualResetEventSlim gate, int count, CancellationToken cancellationToken)
    {
        var polls = new Task<int>[count];
        for (var i = 0; i < polls.Length; i++)
            polls[i] = StartPollerAsync();

        return polls;

        Task<int> StartPollerAsync()
        {
            return Task.Factory.StartNew(
                () => PollReadinessUntilReady(log, gate, cancellationToken),
                CancellationToken.None,
                TaskCreationOptions.LongRunning,
                TaskScheduler.Default);
        }
    }

    private static int PollReadinessUntilReady(FollowerLog log, ManualResetEventSlim gate, CancellationToken cancellationToken)
    {
        _ = gate.Wait(TimeSpan.FromSeconds(5), cancellationToken);

        // A bounded spin: startup completes on the test thread, so termination does not depend on timing.
        var undefined = 0;
        for (var i = 0; i < 1_000_000 && log.Readiness != FollowerLogReadiness.Ready; i++)
        {
            if (log.Readiness is not (FollowerLogReadiness.Unknown or FollowerLogReadiness.Ready or FollowerLogReadiness.Failed))
                undefined++;
        }

        return undefined;
    }
}
