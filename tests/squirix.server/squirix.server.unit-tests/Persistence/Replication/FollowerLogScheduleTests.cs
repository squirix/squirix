using System;
using System.IO;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging.Abstractions;
using Squirix.Server.Attributes;
using Squirix.Server.Storage.Replication;
using Squirix.Server.TestKit;
using Squirix.Server.TestKit.Replication;
using Squirix.Server.UnitTests.Support;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Persistence.Replication;

/// <summary>Leader scheduling notifications respect validation and ambiguous durable failure boundaries.</summary>
[Immutable]
public sealed class FollowerLogScheduleTests : IsolatedStorageTestBase
{
    private const string GroupId = "group";

    /// <summary>Replication cannot start while a higher term is still being persisted.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task NotificationWaitsForDurableTerm(CancellationToken cancellationToken)
    {
        using var hooks = new StallableFollowerLogFaultHooks();
        await using var log = new FollowerLog(Dir, GroupId, GroupComposition.Create(GroupId), NullLogger<FollowerLog>.Instance, hooks, TimeProvider.System);
        await log.OpenSeededAsync(cancellationToken);
        hooks.StallNextMetaWrite();
        var calls = 0;
        var append = log.AppendLeaderAsync(Request() with { CurrentTerm = 2 }, () => { _ = Interlocked.Increment(ref calls); }, cancellationToken);
        try
        {
            await hooks.Entered.WaitAsync(TimeSpan.FromSeconds(15), TimeProvider.System, cancellationToken);
            _ = await Assert.That(Volatile.Read(ref calls)).IsEqualTo(0);
            _ = await Assert.That(append.IsCompleted).IsFalse();
        }
        finally
        {
            hooks.Release();
            Task pendingAppend = append;
            await pendingAppend.ConfigureAwait(ConfigureAwaitOptions.SuppressThrowing);
        }

        _ = await Assert.That((await append).Success).IsTrue();
        _ = await Assert.That(calls).IsEqualTo(1);
        _ = await Assert.That((await log.GetStatusAsync(cancellationToken)).CurrentTerm).IsEqualTo(2UL);
    }

    /// <summary>Only a new accepted frame notifies: neither a durable duplicate nor a stale-term request starts replication again.</summary>
    /// <param name="stale">Whether the second append has a stale term.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task OnlyNewFramesNotify(bool stale, CancellationToken cancellationToken)
    {
        await using var log = new FollowerLog(Dir, GroupId, GroupComposition.Create(GroupId), NullLogger<FollowerLog>.Instance, TimeProvider.System);
        await log.OpenSeededAsync(cancellationToken);
        var calls = 0;
        var notify = () => { _ = Interlocked.Increment(ref calls); };
        var first = await log.AppendLeaderAsync(Request(), notify, cancellationToken);
        var second = await log.AppendLeaderAsync(stale ? Request() with { CurrentTerm = 0 } : Request(), notify, cancellationToken);
        _ = await Assert.That(first.Success).IsTrue();
        _ = await Assert.That(second.Success).IsEqualTo(!stale);
        _ = await Assert.That(calls).IsEqualTo(1);
        _ = await Assert.That((await log.GetStatusAsync(cancellationToken)).LastLogIndex).IsEqualTo(1UL);
    }

    /// <summary>A failure before the durability worker is scheduled never notifies and leaves the next append usable.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task SchedulingRefusalNeverNotifies(CancellationToken cancellationToken)
    {
        using var hooks = new StallableFollowerLogFaultHooks();
        await using var log = new FollowerLog(Dir, GroupId, GroupComposition.Create(GroupId), NullLogger<FollowerLog>.Instance, hooks, TimeProvider.System);
        await log.OpenSeededAsync(cancellationToken);
        var calls = 0;
        var notify = () => { _ = Interlocked.Increment(ref calls); };
        hooks.FailNextFrameScheduling(new IOException("Injected pre-scheduling refusal."));
        _ = await NodeAsyncAssert.ThrowsAsync<IOException>(log.AppendLeaderAsync(Request(), notify, cancellationToken), TimeSpan.FromSeconds(15), TimeProvider.System, cancellationToken);
        _ = await Assert.That(calls).IsEqualTo(0);
        _ = await Assert.That(log.Readiness).IsEqualTo(FollowerLogReadiness.Ready);
        var accepted = await log.AppendLeaderAsync(Request(), notify, cancellationToken);
        _ = await Assert.That(accepted.Success).IsTrue();
        _ = await Assert.That(calls).IsEqualTo(1);
    }

    /// <summary>A canceled admission does not notify or append.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task CanceledAppendNeverNotifies(CancellationToken cancellationToken)
    {
        await using var log = new FollowerLog(Dir, GroupId, GroupComposition.Create(GroupId), NullLogger<FollowerLog>.Instance, TimeProvider.System);
        await log.OpenSeededAsync(cancellationToken);
        using var canceled = new CancellationTokenSource();
        await canceled.CancelAsync();
        var calls = 0;
        var append = log.AppendLeaderAsync(Request(), () => { _ = Interlocked.Increment(ref calls); }, canceled.Token);
        _ = await NodeAsyncAssert.ThrowsAnyAsync<OperationCanceledException>(append, TimeSpan.FromSeconds(15), TimeProvider.System, cancellationToken);
        _ = await Assert.That(calls).IsEqualTo(0);
        _ = await Assert.That((await log.GetStatusAsync(cancellationToken)).LastLogIndex).IsEqualTo(0UL);
    }

    /// <summary>A notification failure still drains the scheduled write, latches ambiguity, and recovers its durable tail.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task NotificationFailureRecoversTail(CancellationToken cancellationToken)
    {
        await using (var log = new FollowerLog(Dir, GroupId, GroupComposition.Create(GroupId), NullLogger<FollowerLog>.Instance, TimeProvider.System))
        {
            await log.OpenSeededAsync(cancellationToken);
            var failure = await NodeAsyncAssert.ThrowsAsync<GroupLogAppendUnconfirmedException>(log.AppendLeaderAsync(Request(), static () => throw new IOException("Injected notification failure."), cancellationToken), TimeSpan.FromSeconds(15), TimeProvider.System, cancellationToken);
            _ = await Assert.That(failure.InnerException).IsTypeOf<IOException>();
            _ = await Assert.That(log.Readiness).IsEqualTo(FollowerLogReadiness.Failed);
            var calls = 0;
            var refused = await log.AppendLeaderAsync(Request(), () => { _ = Interlocked.Increment(ref calls); }, cancellationToken);
            _ = await Assert.That(refused.Success).IsFalse();
            _ = await Assert.That(calls).IsEqualTo(0);
        }

        await using var recovered = new FollowerLog(Dir, GroupId, GroupComposition.Create(GroupId), NullLogger<FollowerLog>.Instance, TimeProvider.System);
        await recovered.OpenAsync(cancellationToken);
        var status = await recovered.GetStatusAsync(cancellationToken);
        _ = await Assert.That((status.LastLogIndex, status.CommitIndex)).IsEqualTo((1UL, 0UL));
    }

    private static FollowerLogAppendRequest Request() => new("leader", 1, 0, 0, 0, new FollowerLogEntry[] { new(1, 1, Encoding.UTF8.GetBytes("payload")) });
}
