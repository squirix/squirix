using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using BenchmarkDotNet.Attributes;
using Squirix.Server.Cluster.Replication;
using Squirix.Server.Storage.Replication;
using Squirix.Server.Utils;

namespace Squirix.Server.Benchmarks;

/// <summary>Cost of owning a finished commit task while the coordinator already owns pending ones, as each commit does under the owned-task lock.</summary>
[MemoryDiagnoser]
[ShortRunJob]
public class ReplicaOwnedTasksBenchmarks
{
    private const int OperationsPerInvoke = 1_000;
    private ReplicaCommitCoordinator? _coordinator;

    /// <summary>Gets or sets the number of owned tasks that never finish.</summary>
    [Params(1, 64, 512)]
    public int PendingTasks { get; set; }

    /// <summary>Disposes the coordinator.</summary>
    /// <returns>A task that completes when cleanup finishes.</returns>
    [GlobalCleanup]
    public async Task CleanupAsync()
    {
        if (_coordinator != null)
            await _coordinator.DisposeAsync().ConfigureAwait(false);
        _coordinator = null;
    }

    /// <summary>Owns finished tasks one after another, the way commits that complete do.</summary>
    /// <exception cref="InvalidOperationException">Thrown when the benchmark was not initialized.</exception>
    [Benchmark(OperationsPerInvoke = OperationsPerInvoke)]
    public void OwnCompleted()
    {
        var coordinator = ThrowHelper.Required(_coordinator, "Benchmark coordinator was not initialized.");
        var completed = Task.CompletedTask;
        for (var i = 0; i < OperationsPerInvoke; i++)
        {
            lock (coordinator.OwnedSync)
                coordinator.OwnCore(completed);
        }
    }

    /// <summary>Creates the coordinator and owns the pending tasks.</summary>
    [GlobalSetup]
    public void Setup()
    {
        var options = new ReplicaCommitCoordinatorOptions(3, 0, 0, 2);
        var idempotency = new GroupIdempotencyState(4, TimeSpan.MaxValue, TimeProvider.System);
        _coordinator = new ReplicaCommitCoordinator(options, new IdlePipeline(), new IdleHooks(), idempotency)
        {
            BudgetTimeProvider = TimeProvider.System,
            ObserveTimeProvider = TimeProvider.System,
            ShutdownBudget = TimeSpan.FromMilliseconds(1),
        };
        for (var i = 0; i < PendingTasks; i++)
            _coordinator.Own(new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously).Task);
    }

    /// <summary>Fault hooks that never interfere.</summary>
    private sealed class IdleHooks : IReplicaCommitFaultHooks
    {
        public ValueTask OnStageAsync(ReplicaCommitStage stage, PreparedReplicaMutation mutation, CancellationToken cancellationToken) => ValueTask.CompletedTask;
    }

    /// <summary>Pipeline the benchmark never drives.</summary>
    private sealed class IdlePipeline : IReplicaCommitPipeline
    {
        public ValueTask AdvanceCommitIndexAsync(ulong commitIndex, CancellationToken cancellationToken) => ValueTask.CompletedTask;

        public ValueTask AppendLocalAsync(IReadOnlyList<PreparedReplicaMutation> mutations, CancellationToken cancellationToken) => ValueTask.CompletedTask;

        public ValueTask<ReplicaDurableAcknowledgement> AppendFollowerAsync(int replicaIndex, PreparedReplicaMutation mutation, CancellationToken cancellationToken) =>
            throw new InvalidOperationException("The benchmark sends no entry.");

        public ValueTask ApplyMemoryAsync(PreparedReplicaMutation mutation, CancellationToken cancellationToken) => ValueTask.CompletedTask;

        public void RecordLaggingReplica(int replicaIndex, ulong logIndex)
        {
        }
    }
}
