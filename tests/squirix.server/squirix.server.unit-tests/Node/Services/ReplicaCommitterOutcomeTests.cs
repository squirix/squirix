using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging.Abstractions;
using Squirix.Server.Cluster.Replication;
using Squirix.Server.Core;
using Squirix.Server.Errors;
using Squirix.Server.Node.Services;
using Squirix.Server.Storage.Replication;
using Squirix.Server.TestKit;
using Squirix.Server.UnitTests.Support;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;
using static Squirix.Server.UnitTests.Node.Services.ReplicaCommitterDoubles;

namespace Squirix.Server.UnitTests.Node.Services;

/// <summary>
/// A replicated write that fails at its local append tells the caller the truth: refused when nothing reached the log, unknown when the
/// log already holds the entry.
/// </summary>
public sealed class ReplicaCommitterOutcomeTests : IsolatedStorageTestBase
{
    private const string OwnedGroup = "n1";

    private static readonly byte[] Fingerprint = [9, 8, 7];

    /// <summary>A commit budget much shorter than the stall the second write waits behind.</summary>
    private static readonly TimeSpan ShortCommitBudget = TimeSpan.FromMilliseconds(200);

    private static readonly TimeSpan StallTimeout = TimeSpan.FromSeconds(10);

    /// <summary>A commit budget that expires while the write waits for the log, before its append, is a retryable refusal.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task BudgetBeforeAppendIsRetryable(CancellationToken cancellationToken)
    {
        using var hooks = new StallableFollowerLogFaultHooks();
        await using var registry = await OpenRegistryAsync(hooks, cancellationToken);
        var clock = new DueTimerClock(ShortCommitBudget);
        await using var committer = CreateCommitter(registry, clock);
        await committer.CommitSetAsync(Guid.NewGuid().ToString("N"), "cache", "k1", Entry(), cancellationToken);
        var log = Log(registry);

        // A metadata write parked under the log gate keeps the next append waiting for it well past the commit budget.
        hooks.StallNextMetaWrite();
        var holder = log.AdvanceAppliedAsync(1UL, cancellationToken);
        await hooks.Entered.WaitAsync(StallTimeout, TimeProvider.System, cancellationToken);

        // The budget runs on the fake clock: advance it past the budget once the second write armed it, while the write still waits for the log.
        clock.ForgetCreated();
        var write = committer.CommitSetAsync(Guid.NewGuid().ToString("N"), "cache", "k2", Entry(), cancellationToken);
        _ = await Assert.That(await clock.TimerCreated.WaitAsync(StallTimeout, cancellationToken)).IsTrue();
        clock.Advance(ShortCommitBudget * 5);
        hooks.Release();
        _ = await holder.WaitAsync(StallTimeout, TimeProvider.System, cancellationToken);

        var refused = await NodeAsyncAssert.ThrowsAsync<SquirixException>(write.WaitAsync(StallTimeout, TimeProvider.System, cancellationToken));

        _ = await Assert.That(refused.Code).IsEqualTo(SquirixErrorCode.TooManyRequests);
        _ = await Assert.That((await log.GetStatusAsync(cancellationToken)).LastLogIndex).IsEqualTo(1UL);
    }

    /// <summary>An append whose frames reached the disk before its metadata write failed is an unknown outcome, never a refusal.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task DurableAppendFailureIsUnknown(CancellationToken cancellationToken)
    {
        using var hooks = new StallableFollowerLogFaultHooks();
        await using var registry = await OpenRegistryAsync(hooks, cancellationToken);
        await using var committer = CreateCommitter(registry, TimeProvider.System);
        await committer.CommitSetAsync(Guid.NewGuid().ToString("N"), "cache", "k1", Entry(), cancellationToken);

        hooks.StallNextMetaWrite();
        var write = committer.CommitSetAsync(Guid.NewGuid().ToString("N"), "cache", "k2", Entry(), cancellationToken);
        await hooks.Entered.WaitAsync(StallTimeout, TimeProvider.System, cancellationToken);
        hooks.ReleaseWithFailure(new IOException("Injected metadata write failure."));

        var error = await NodeAsyncAssert.ThrowsAsync<SquirixException>(write.WaitAsync(StallTimeout, TimeProvider.System, cancellationToken));

        _ = await Assert.That(error.Code).IsEqualTo(SquirixErrorCode.CommitOutcomeUnknown);
        _ = await Assert.That((await Log(registry).GetStatusAsync(cancellationToken)).LastLogIndex).IsEqualTo(2UL);
    }

    /// <summary>A failed append whose entry the log already holds is an unknown outcome even when the failure reads as a stale-term refusal.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task HeldEntryWinsOverStaleTerm(CancellationToken cancellationToken)
    {
        using var hooks = new StallableFollowerLogFaultHooks();
        await using var registry = await OpenRegistryAsync(hooks, cancellationToken);
        await using var committer = CreateCommitter(registry, TimeProvider.System);
        await committer.CommitSetAsync(Guid.NewGuid().ToString("N"), "cache", "k1", Entry(), cancellationToken);

        hooks.StallNextMetaWrite();
        var write = committer.CommitSetAsync(Guid.NewGuid().ToString("N"), "cache", "k2", Entry(), cancellationToken);
        await hooks.Entered.WaitAsync(StallTimeout, TimeProvider.System, cancellationToken);
        hooks.ReleaseWithFailure(new InvalidOperationException(ReplicaGroupCommitPipeline.LocalAppendStaleTermMessage));

        var error = await NodeAsyncAssert.ThrowsAsync<SquirixException>(write.WaitAsync(StallTimeout, TimeProvider.System, cancellationToken));

        _ = await Assert.That(error.Code).IsEqualTo(SquirixErrorCode.CommitOutcomeUnknown);
        _ = await Assert.That((await Log(registry).GetStatusAsync(cancellationToken)).LastLogIndex).IsEqualTo(2UL);
    }

    /// <summary>A committed identifier reused with another request is reported as a reuse, and the group keeps committing.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task CommittedIdReuseIsMismatch(CancellationToken cancellationToken)
    {
        using var hooks = new StallableFollowerLogFaultHooks();
        await using var registry = await OpenRegistryAsync(hooks, cancellationToken);
        await using var committer = CreateCommitter(registry, TimeProvider.System);
        var operationId = Guid.NewGuid().ToString("N");
        await committer.CommitSetAsync(operationId, "cache", "k1", Entry(), cancellationToken);

        _ = await NodeAsyncAssert.ThrowsAsync<ServerOpIdMismatchException>(committer.CommitSetAsync(operationId, "cache", "k2", Entry(), cancellationToken));
        await committer.CommitSetAsync(Guid.NewGuid().ToString("N"), "cache", "k3", Entry(), cancellationToken);

        _ = await Assert.That((await Log(registry).GetStatusAsync(cancellationToken)).LastLogIndex).IsEqualTo(2UL);
    }

    private static ReplicaGroupCommitter CreateCommitter(ReplicaGroupRegistry registry, TimeProvider budgetClock)
    {
        var local = new ScriptedApplyCache(ApplyMode.Fail);
        local.Recover();
        return new ReplicaGroupCommitter(registry, new TwoNodeLocator(), new AcceptingGateway(), local, (OwnedGroup, OwnedGroup), new ReplicaTopologyStamp(Fingerprint, 1), NullLogger<ReplicaGroupCommitter>.Instance)
        {
            Recovery = RecoveryLifecycle.Recovered(),
            Applier = new ReplicaGroupApplier(local, NullLogger.Instance, OwnedGroup, OwnedGroup),
            CommitBudget = ShortCommitBudget,
            BudgetTimeProvider = budgetClock,
            ShutdownBudget = TimeSpan.FromMilliseconds(200),
        };
    }

    private static NodeCacheEntry<object?> Entry() => new() { Value = "v", Version = 1 };

    private static IFollowerLog Log(ReplicaGroupRegistry registry) =>
        registry.TryGetLog(OwnedGroup, out var log) ? log : throw new InvalidOperationException("The owned group log is not open.");

    private async Task<ReplicaGroupRegistry> OpenRegistryAsync(StallableFollowerLogFaultHooks hooks, CancellationToken cancellationToken)
    {
        var options = new FollowerLogOptions { FaultHooks = hooks, ShutdownBudget = TimeSpan.FromMilliseconds(200) };
        var registry = new ReplicaGroupRegistry(Dir, [OwnedGroup], 2, Fingerprint, 1, NullLoggerFactory.Instance, options);
        try
        {
            await registry.OpenAsync(cancellationToken);
        }
        catch
        {
            await registry.DisposeAsync();
            throw;
        }

        return registry;
    }
}
