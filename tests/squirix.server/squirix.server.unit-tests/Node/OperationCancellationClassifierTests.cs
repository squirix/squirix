using System;
using System.Threading;
using System.Threading.Tasks;
using Grpc.Core;
using Squirix.Server.Node;
using Squirix.Server.Node.App.Decorators;
using Xunit;

namespace Squirix.Server.UnitTests.Node;

/// <summary>
/// Characterization tests for <see cref="OperationCancellationClassifier" /> precedence and transport helpers.
/// </summary>
public sealed class OperationCancellationClassifierTests : ServerUnitTestBase
{
    private const int CallerCanceledOrdinal = (int)CancellationScenarioKind.CallerCanceled;
    private const int OperationDeadlineExceededOrdinal = (int)CancellationScenarioKind.OperationDeadlineExceeded;

    /// <summary>
    /// Caller cancellation wins over operation deadline and per-attempt signals in classification precedence.
    /// </summary>
    /// <param name="callerCanceled">Simulated caller token canceled state.</param>
    /// <param name="operationEffectiveCanceled">Simulated operation effective token canceled state.</param>
    /// <param name="perAttemptScopeCanceled">Simulated per-attempt composite token canceled state.</param>
    /// <param name="expectedOrdinal">Expected <see cref="CancellationScenarioKind" /> ordinal.</param>
    [Theory]
    [InlineData(true, true, true, CallerCanceledOrdinal)]
    [InlineData(true, true, false, CallerCanceledOrdinal)]
    [InlineData(true, false, true, CallerCanceledOrdinal)]
    [InlineData(true, false, false, CallerCanceledOrdinal)]
    public void ClassifyFromLinkedTokenStateCallerCanceledWinsOverDeadlineAndAttempt(
        bool callerCanceled,
        bool operationEffectiveCanceled,
        bool perAttemptScopeCanceled,
        int expectedOrdinal)
    {
        var expected = (CancellationScenarioKind)expectedOrdinal;
        var actual = OperationCancellationClassifier.ClassifyFromLinkedTokenState(callerCanceled, operationEffectiveCanceled, perAttemptScopeCanceled);
        Assert.Equal(expected, actual);
    }

    /// <summary>
    /// Operation deadline is classified when the caller is not canceled.
    /// </summary>
    /// <param name="callerCanceled">Simulated caller token canceled state.</param>
    /// <param name="operationEffectiveCanceled">Simulated operation effective token canceled state.</param>
    /// <param name="perAttemptScopeCanceled">Simulated per-attempt composite token canceled state.</param>
    /// <param name="expectedOrdinal">Expected <see cref="CancellationScenarioKind" /> ordinal.</param>
    [Theory]
    [InlineData(false, true, true, OperationDeadlineExceededOrdinal)]
    [InlineData(false, true, false, OperationDeadlineExceededOrdinal)]
    public void ClassifyFromLinkedTokenStateOperationDeadlinePrecedesPerAttemptWhenBothSet(
        bool callerCanceled,
        bool operationEffectiveCanceled,
        bool perAttemptScopeCanceled,
        int expectedOrdinal)
    {
        var expected = (CancellationScenarioKind)expectedOrdinal;
        var actual = OperationCancellationClassifier.ClassifyFromLinkedTokenState(callerCanceled, operationEffectiveCanceled, perAttemptScopeCanceled);
        Assert.Equal(expected, actual);
    }

    /// <summary>
    /// Per-attempt timeout is classified only when caller and operation effective tokens are not canceled.
    /// </summary>
    [Fact]
    public void ClassifyFromLinkedTokenStatePerAttemptTimedOutWhenOnlyAttemptScopeCanceled()
    {
        var kind = OperationCancellationClassifier.ClassifyFromLinkedTokenState(false, false, true);
        Assert.Equal(CancellationScenarioKind.PerAttemptTimedOut, kind);
    }

    /// <summary>
    /// When no structured token is canceled, classification is unknown cancellation.
    /// </summary>
    [Fact]
    public void ClassifyFromLinkedTokenStateUnknownWhenNoTokenCanceled()
    {
        var kind = OperationCancellationClassifier.ClassifyFromLinkedTokenState(false, false, false);
        Assert.Equal(CancellationScenarioKind.UnknownCancellation, kind);
    }

    /// <summary>
    /// Pipeline deadline helper matches the same precedence as the raw three-bool overload for the two-token layout.
    /// </summary>
    [Fact]
    public void ClassifyLogicalPipelineDeadlineCancellationMatchesRawClassification()
    {
        using var linkedCts = new CancellationTokenSource();
        linkedCts.Cancel();
        var raw = OperationCancellationClassifier.ClassifyFromLinkedTokenState(false, true, false);
        var helper = OperationCancellationClassifier.ClassifyLogicalPipelineDeadlineCancellation(CancellationToken.None, linkedCts.Token);
        Assert.Equal(raw, helper);
    }

    /// <summary>
    /// Peer call attempt helper matches the raw three-bool classification for the same token states.
    /// </summary>
    [Fact]
    public void ClassifyPeerCallAttemptCancellationMatchesRawClassification()
    {
        using var attemptCts = new CancellationTokenSource();
        attemptCts.Cancel();
        var raw = OperationCancellationClassifier.ClassifyFromLinkedTokenState(false, false, true);
        using var outer = new CancellationTokenSource();
        using var eff = new CancellationTokenSource();
        var helper = OperationCancellationClassifier.ClassifyPeerCallAttemptCancellation(outer.Token, eff.Token, attemptCts.Token);
        Assert.Equal(raw, helper);
    }

    /// <summary>
    /// Domain transport mapper still maps gRPC Cancelled plus caller token to caller cancellation.
    /// </summary>
    /// <returns>A task that completes when assertions pass.</returns>
    [Fact]
    public async Task DomainTransportErrorMapperStillMapsGrpcCancelledWithCallerTokenToOperationCanceled()
    {
        using var cts = new CancellationTokenSource();
        await cts.CancelAsync();
        var callerToken = cts.Token;
        var ex = new RpcException(new Status(StatusCode.Cancelled, "call canceled"));
        _ = await Assert.ThrowsAnyAsync<OperationCanceledException>(() => Task.Run(() => DomainTransportErrorMapper.Map(ex, callerToken), CancellationToken.None));
    }

    /// <summary>
    /// gRPC caller cancellation is detected only when status is Cancelled and the caller token is canceled.
    /// </summary>
    [Fact]
    public void IsCallerInitiatedGrpcCancellationRequiresCancelledStatusAndCallerToken()
    {
        using var cts = new CancellationTokenSource();
        cts.Cancel();
        var ex = new RpcException(new Status(StatusCode.Cancelled, "x"));
        Assert.True(OperationCancellationClassifier.IsCallerInitiatedGrpcCancellation(ex, cts.Token));
        Assert.False(OperationCancellationClassifier.IsCallerInitiatedGrpcCancellation(ex, CancellationToken.None));
        var other = new RpcException(new Status(StatusCode.DeadlineExceeded, "x"));
        Assert.False(OperationCancellationClassifier.IsCallerInitiatedGrpcCancellation(other, cts.Token));
    }

    /// <summary>
    /// Cooperative watch shutdown treats Unavailable as well as Cancelled when the caller token is canceled.
    /// </summary>
    [Fact]
    public void IsCallerInitiatedGrpcWatchStreamFaultIncludesUnavailableWhenCallerCanceled()
    {
        using var cts = new CancellationTokenSource();
        cts.Cancel();
        var unavailable = new RpcException(new Status(StatusCode.Unavailable, "Error reading next message. IOException: The request was aborted."));
        var cancelled = new RpcException(new Status(StatusCode.Cancelled, "x"));
        Assert.True(OperationCancellationClassifier.IsCallerInitiatedGrpcWatchStreamFault(unavailable, cts.Token));
        Assert.True(OperationCancellationClassifier.IsCallerInitiatedGrpcWatchStreamFault(cancelled, cts.Token));
        Assert.False(OperationCancellationClassifier.IsCallerInitiatedGrpcWatchStreamFault(unavailable, CancellationToken.None));
        var deadline = new RpcException(new Status(StatusCode.DeadlineExceeded, "x"));
        Assert.False(OperationCancellationClassifier.IsCallerInitiatedGrpcWatchStreamFault(deadline, cts.Token));
    }

    /// <summary>
    /// Operation effective token helper mirrors not canceled for retry gating.
    /// </summary>
    [Fact]
    public void OperationEffectiveTokenAllowsRetryAttemptReflectsTokenState()
    {
        using var cts = new CancellationTokenSource();
        Assert.True(OperationCancellationClassifier.OperationEffectiveTokenAllowsRetryAttempt(cts.Token));
        cts.Cancel();
        Assert.False(OperationCancellationClassifier.OperationEffectiveTokenAllowsRetryAttempt(cts.Token));
    }
}
