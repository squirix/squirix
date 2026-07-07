using System;
using System.Threading;
using System.Threading.Tasks;
using Grpc.Core;
using Squirix.Server.Node;
using Squirix.Server.Node.App.Decorators;
using Squirix.Server.UnitTests.Support;
using Xunit;

namespace Squirix.Server.UnitTests.Core;

/// <summary>
/// Characterization tests for <see cref="ServerCancelClassifier" /> precedence and transport helpers.
/// </summary>
public sealed class OperationCancellationClassifierTests : UnitTestBase
{
    /// <summary>Domain transport mapper still maps gRPC Canceled plus caller token to caller cancellation.</summary>
    [Fact]
    public async Task DomainTransportErrorMapperStillMapsGrpcCanceledWithCallerTokenToOperationCanceled()
    {
        using var cts = new CancellationTokenSource();
        await cts.CancelAsync();
        var callerToken = cts.Token;
        var ex = new RpcException(new Status(StatusCode.Cancelled, "call canceled"));
        _ = await Assert.ThrowsAnyAsync<OperationCanceledException>(async () =>
        {
            await Task.Factory.StartNew(
                () => DomainTransportErrorMapper.Map(ex, callerToken),
                CancellationToken.None,
                TaskCreationOptions.DenyChildAttach,
                TaskScheduler.Default);
        });
    }

    /// <summary>gRPC caller cancellation is detected only when status is Canceled and the caller token is canceled.</summary>
    [Fact]
    public void IsCallerInitiatedGrpcCancellationRequiresCanceledStatusAndCallerToken()
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
    public void OperationEffectiveTokenAllowsRetryAttemptReflectsTokenState()
    {
        using var cts = new CancellationTokenSource();
        Assert.True(ServerCancelClassifier.OperationEffectiveTokenAllowsRetryAttempt(cts.Token));
        cts.Cancel();
        Assert.False(ServerCancelClassifier.OperationEffectiveTokenAllowsRetryAttempt(cts.Token));
    }
}
