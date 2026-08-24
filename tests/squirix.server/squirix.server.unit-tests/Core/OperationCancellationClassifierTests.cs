using System.Threading;
using Grpc.Core;
using Squirix.Server.Attributes;
using Squirix.Server.Node.Observability;
using Squirix.Server.UnitTests.Support;
using Xunit;

namespace Squirix.Server.UnitTests.Core;

/// <summary>
/// Characterization tests for <see cref="ServerCancelClassifier" /> precedence and transport helpers.
/// </summary>
[Immutable]
public sealed class OperationCancellationClassifierTests : ServerUnitTestBase
{
    /// <summary>gRPC caller cancellation is detected only when status is Canceled and the caller token is canceled.</summary>
    [Fact]
    public void CallerTokenWithCanceledStatusIsCaller()
    {
        using var cts = new CancellationTokenSource();
        cts.Cancel();
        var ex = new RpcException(new Status(StatusCode.Cancelled, "x"));
        Assert.True(ServerCancelClassifier.IsCallerInitiatedGrpcCancellation(ex, cts.Token));
        Assert.False(ServerCancelClassifier.IsCallerInitiatedGrpcCancellation(ex, CancellationToken.None));
        var other = new RpcException(new Status(StatusCode.DeadlineExceeded, "x"));
        Assert.False(ServerCancelClassifier.IsCallerInitiatedGrpcCancellation(other, cts.Token));
    }

    /// <summary>Operation effective token helper mirrors not canceled for retry gating.</summary>
    [Fact]
    public void AllowsRetryReflectsEffectiveTokenState()
    {
        using var cts = new CancellationTokenSource();
        Assert.True(ServerCancelClassifier.EffectiveTokenAllowsRetryAttempt(cts.Token));
        cts.Cancel();
        Assert.False(ServerCancelClassifier.EffectiveTokenAllowsRetryAttempt(cts.Token));
    }
}
