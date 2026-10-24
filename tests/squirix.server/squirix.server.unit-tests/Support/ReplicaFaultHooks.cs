using System.Threading;
using System.Threading.Tasks;
using Rocks;
using Squirix.Server.Cluster.Replication;

namespace Squirix.Server.UnitTests.Support;

/// <summary>Rocks-backed replica commit fault hooks shared by replication tests.</summary>
internal static class ReplicaFaultHooks
{
    /// <summary>Creates fault hooks that honor their token, as any budget-aware step would: a canceled token faults the stage.</summary>
    internal static IReplicaCommitFaultHooks CreateCancellationHonoring()
    {
        var expectations = new IReplicaCommitFaultHooksCreateExpectations();
        _ = expectations.Setups.OnStageAsync(Arg.Any<ReplicaCommitStage>(), Arg.Any<PreparedReplicaMutation>(), Arg.Any<CancellationToken>())
                        .Callback(static (_, _, cancellationToken) =>
                            cancellationToken.IsCancellationRequested ? ValueTask.FromCanceled(cancellationToken) : ValueTask.CompletedTask);
        return expectations.Instance();
    }

    /// <summary>Creates fault hooks that signal when a commit reaches a stage and, when given a release, hold it there until the release completes.</summary>
    /// <param name="stage">The stage to signal.</param>
    /// <param name="entered">Completed when the stage is reached.</param>
    /// <param name="release">Completes to let the stage return, or <see langword="null" /> to return at once.</param>
    /// <returns>The hooks.</returns>
    internal static IReplicaCommitFaultHooks CreateAtStage(ReplicaCommitStage stage, TaskCompletionSource entered, Task? release = null)
    {
        var expectations = new IReplicaCommitFaultHooksCreateExpectations();
        _ = expectations.Setups.OnStageAsync(Arg.Any<ReplicaCommitStage>(), Arg.Any<PreparedReplicaMutation>(), Arg.Any<CancellationToken>())
                        .Callback((reached, _, _) =>
                        {
                            if (reached != stage)
                                return ValueTask.CompletedTask;

                            _ = entered.TrySetResult();
                            return release == null ? ValueTask.CompletedTask : new ValueTask(release);
                        });
        return expectations.Instance();
    }

    /// <summary>Creates fault hooks that never interfere with any commit stage.</summary>
    internal static IReplicaCommitFaultHooks CreateNoOp()
    {
        var expectations = new IReplicaCommitFaultHooksCreateExpectations();
        _ = expectations.Setups.OnStageAsync(Arg.Any<ReplicaCommitStage>(), Arg.Any<PreparedReplicaMutation>(), Arg.Any<CancellationToken>())
                        .ReturnValue(ValueTask.CompletedTask);
        return expectations.Instance();
    }
}
