using System;
using System.Collections.Generic;
using System.Globalization;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Cluster.Replication;
using Squirix.Server.Storage.Replication;
using Squirix.Server.TestKit;
using Squirix.Server.UnitTests.Support;

namespace Squirix.Server.UnitTests.Cluster.Replication;

/// <summary>Shared builders and waits of the tests of the dense-run commit.</summary>
internal static class ReplicaBatchKit
{
    /// <summary>The commit budget of a test that expires it on a fake clock.</summary>
    internal static readonly TimeSpan CommitBudget = TimeSpan.FromMilliseconds(200);

    /// <summary>The longest real wait of a test for something that must happen.</summary>
    internal static readonly TimeSpan StallTimeout = TimeSpan.FromSeconds(10);

    /// <summary>Commits a run with the stall timeout as its budget.</summary>
    /// <param name="coordinator">The coordinator.</param>
    /// <param name="run">The run.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    /// <returns>The commit.</returns>
    internal static Task CommitAsync(ReplicaCommitCoordinator coordinator, IReadOnlyList<PreparedReplicaMutation> run, CancellationToken cancellationToken) =>
        coordinator.CommitBatchAsync(run, StallTimeout, cancellationToken).AsTask();

    /// <summary>Starts a run with the commit budget.</summary>
    /// <param name="coordinator">The coordinator.</param>
    /// <param name="run">The run.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    /// <returns>The started commit.</returns>
    internal static Task StartAsync(ReplicaCommitCoordinator coordinator, IReadOnlyList<PreparedReplicaMutation> run, CancellationToken cancellationToken) =>
        coordinator.CommitBatchAsync(run, CommitBudget, cancellationToken).AsTask();

    /// <summary>Starts a run on a budget that a fake clock expires, then expires it.</summary>
    /// <param name="coordinator">The coordinator whose budget clock is <paramref name="clock" />.</param>
    /// <param name="run">The run.</param>
    /// <param name="clock">The budget clock.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    /// <returns>The failure of the run.</returns>
    internal static Task<InvalidOperationException> ExpireAsync(ReplicaCommitCoordinator coordinator, PreparedReplicaMutation[] run, DueTimerClock clock, CancellationToken cancellationToken) =>
        ExpireAsync(StartAsync(coordinator, run, cancellationToken), clock, cancellationToken);

    /// <summary>Expires the budget of a started commit and returns its failure.</summary>
    /// <param name="commit">The started commit.</param>
    /// <param name="clock">The budget clock.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    /// <returns>The failure of the commit.</returns>
    internal static async Task<InvalidOperationException> ExpireAsync(Task commit, DueTimerClock clock, CancellationToken cancellationToken)
    {
        _ = await clock.TimerCreated.WaitAsync(StallTimeout, cancellationToken);
        clock.Advance(CommitBudget);
        return await NodeAsyncAssert.ThrowsAsync<InvalidOperationException>(commit.WaitAsync(StallTimeout, TimeProvider.System, cancellationToken));
    }

    /// <summary>Describes the idempotency record of each entry: F found, U unresolved, M missing, X conflicting.</summary>
    /// <param name="idempotency">The idempotency state.</param>
    /// <param name="run">The entries.</param>
    /// <returns>One letter per entry.</returns>
    /// <exception cref="ArgumentOutOfRangeException">The lookup result is not a known one.</exception>
    internal static string LookupStates(GroupIdempotencyState idempotency, PreparedReplicaMutation[] run)
    {
        var states = new char[run.Length];
        for (var i = 0; i < run.Length; i++)
        {
            var mutation = run[i];
            var lookup = idempotency.Lookup(mutation.OperationScope, mutation.OperationId, mutation.OperationFingerprint.Span, out _);
            states[i] = lookup switch
            {
                GroupIdempotencyLookup.Found => 'F',
                GroupIdempotencyLookup.Unresolved => 'U',
                GroupIdempotencyLookup.Miss => 'M',
                GroupIdempotencyLookup.Mismatch => 'X',
                _ => throw new ArgumentOutOfRangeException(nameof(run), lookup, "Unsupported lookup."),
            };
        }

        return new string(states);
    }

    /// <summary>Creates an idempotency state with room for a few runs.</summary>
    /// <returns>The state.</returns>
    internal static GroupIdempotencyState NewIdempotency() => new(16, TimeSpan.MaxValue, TimeProvider.System);

    /// <summary>Creates a coordinator over the pipeline, led by slot zero.</summary>
    /// <param name="pipeline">The pipeline.</param>
    /// <param name="idempotency">The idempotency state.</param>
    /// <param name="budgetClock">The clock of the commit budget.</param>
    /// <param name="replicaCount">The replica count, leader included.</param>
    /// <param name="hooks">The fault hooks, or <see langword="null" /> for none.</param>
    /// <returns>The coordinator.</returns>
    internal static ReplicaCommitCoordinator NewCoordinator(
        RunPipeline pipeline,
        GroupIdempotencyState idempotency,
        TimeProvider budgetClock,
        int replicaCount = 2,
        IReplicaCommitFaultHooks? hooks = null) =>
        new(new ReplicaCommitCoordinatorOptions(replicaCount, 0, 0, 2), pipeline, hooks ?? ReplicaFaultHooks.CreateNoOp(), idempotency)
        {
            ObserveTimeProvider = TimeProvider.System,
            BudgetTimeProvider = budgetClock,
        };

    /// <summary>Creates a mutation whose operation identity is derived from its log index.</summary>
    /// <param name="logIndex">The log index.</param>
    /// <param name="term">The term.</param>
    /// <returns>The mutation.</returns>
    internal static PreparedReplicaMutation Mutation(ulong logIndex, ulong term = 1) => Mutation(logIndex, term, logIndex);

    /// <summary>Creates a mutation whose operation identity is derived from another number than its log index.</summary>
    /// <param name="logIndex">The log index.</param>
    /// <param name="term">The term.</param>
    /// <param name="identity">The number the operation identity is derived from.</param>
    /// <returns>The mutation.</returns>
    internal static PreparedReplicaMutation Mutation(ulong logIndex, ulong term, ulong identity) => new(
        new ReplicaOperationIdentity("group-a", "client", identity.ToString("x32", CultureInfo.InvariantCulture), new byte[] { 1 }),
        term,
        logIndex,
        new ReplicaMutationPayload(new byte[] { 2 }, new[] { byte.CreateChecked(10 + logIndex) }, 4));

    /// <summary>Creates a dense run of one term with identities derived from the log indexes.</summary>
    /// <param name="first">The first log index.</param>
    /// <param name="count">The number of entries.</param>
    /// <returns>The run.</returns>
    internal static PreparedReplicaMutation[] Run(ulong first, int count)
    {
        var run = new PreparedReplicaMutation[count];
        for (var i = 0; i < count; i++)
            run[i] = Mutation(first + ulong.CreateChecked(i));

        return run;
    }
}
