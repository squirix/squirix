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

    /// <summary>Creates fault hooks that never interfere with any commit stage.</summary>
    internal static IReplicaCommitFaultHooks CreateNoOp()
    {
        var expectations = new IReplicaCommitFaultHooksCreateExpectations();
        _ = expectations.Setups.OnStageAsync(Arg.Any<ReplicaCommitStage>(), Arg.Any<PreparedReplicaMutation>(), Arg.Any<CancellationToken>())
                        .ReturnValue(ValueTask.CompletedTask);
        return expectations.Instance();
    }
}
