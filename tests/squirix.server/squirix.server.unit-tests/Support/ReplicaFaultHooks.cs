using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Cluster.Replication;
using TUnit.Mocks;
using TUnit.Mocks.Arguments;
using TUnit.Mocks.Generated;

namespace Squirix.Server.UnitTests.Support;

/// <summary>Replica commit fault hooks shared by replication tests.</summary>
internal static class ReplicaFaultHooks
{
    /// <summary>Creates fault hooks that honor their token, as any budget-aware step would: a canceled token faults the stage.</summary>
    internal static IReplicaCommitFaultHooks CreateCancellationHonoring()
    {
        var expectations = IReplicaCommitFaultHooks.Mock(MockBehavior.Strict);
        _ = expectations.OnStageAsync(Arg.Any<ReplicaCommitStage>(), Arg.Any<PreparedReplicaMutation>(), Arg.Any<CancellationToken>())
                        .ReturnsAsync(static (_, _, cancellationToken) =>
                            cancellationToken.IsCancellationRequested ? ValueTask.FromCanceled(cancellationToken) : ValueTask.CompletedTask);
        return expectations.Object;
    }

    /// <summary>Creates fault hooks that signal when a commit reaches a stage and, when given a release, hold it there until the release completes.</summary>
    /// <param name="stage">The stage to signal.</param>
    /// <param name="entered">Completed when the stage is reached.</param>
    /// <param name="release">Completes to let the stage return, or <see langword="null" /> to return at once.</param>
    /// <returns>The hooks.</returns>
    internal static IReplicaCommitFaultHooks CreateAtStage(ReplicaCommitStage stage, TaskCompletionSource entered, Task? release = null)
    {
        var expectations = IReplicaCommitFaultHooks.Mock(MockBehavior.Strict);
        _ = expectations.OnStageAsync(Arg.Any<ReplicaCommitStage>(), Arg.Any<PreparedReplicaMutation>(), Arg.Any<CancellationToken>())
                        .ReturnsAsync((reached, _, _) =>
                        {
                            if (reached != stage)
                                return ValueTask.CompletedTask;

                            _ = entered.TrySetResult();
                            return release == null ? ValueTask.CompletedTask : new ValueTask(release);
                        });
        return expectations.Object;
    }

    /// <summary>Creates fault hooks that never interfere with any commit stage.</summary>
    internal static IReplicaCommitFaultHooks CreateNoOp()
    {
        var expectations = IReplicaCommitFaultHooks.Mock(MockBehavior.Strict);
        _ = expectations.OnStageAsync(Arg.Any<ReplicaCommitStage>(), Arg.Any<PreparedReplicaMutation>(), Arg.Any<CancellationToken>())
                        .ReturnsAsync(ValueTask.CompletedTask);
        return expectations.Object;
    }
}
