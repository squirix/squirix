using System.Threading;
using System.Threading.Tasks;
using Grpc.Core;
using Squirix.Server.Attributes;
using Squirix.Server.Node.Observability;
using Squirix.Server.UnitTests.Support;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Core;

/// <summary>Characterization tests for <see cref="ServerCancelClassifier" /> precedence and transport helpers.</summary>
[Immutable]
public sealed class OperationCancellationClassifierTests : ServerUnitTestBase
{
    /// <summary>Operation effective token helper mirrors not canceled for retry gating.</summary>
    [Test]
    public async Task AllowsRetryReflectsEffectiveTokenState()
    {
        using var cts = new CancellationTokenSource();
        _ = await Assert.That(ServerCancelClassifier.EffectiveTokenAllowsRetryAttempt(cts.Token)).IsTrue();
        await cts.CancelAsync();
        _ = await Assert.That(ServerCancelClassifier.EffectiveTokenAllowsRetryAttempt(cts.Token)).IsFalse();
    }

    /// <summary>gRPC caller cancellation is detected only when status is Canceled and the caller token is canceled.</summary>
    [Test]
    public async Task CallerTokenWithCanceledStatusIsCaller()
    {
        using var cts = new CancellationTokenSource();
        await cts.CancelAsync();
        var ex = new RpcException(new Status(StatusCode.Cancelled, "x"));
        _ = await Assert.That(ServerCancelClassifier.IsCallerInitiatedGrpcCancellation(ex, cts.Token)).IsTrue();
        _ = await Assert.That(ServerCancelClassifier.IsCallerInitiatedGrpcCancellation(ex, CancellationToken.None)).IsFalse();
        var other = new RpcException(new Status(StatusCode.DeadlineExceeded, "x"));
        _ = await Assert.That(ServerCancelClassifier.IsCallerInitiatedGrpcCancellation(other, cts.Token)).IsFalse();
    }
}
