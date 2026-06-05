using System.Threading;
using Grpc.Core;

namespace Squirix.Server.Node;

/// <summary>
/// Canonical classification for cancellation and timeout signals expressed through linked <see cref="CancellationToken" /> state.
/// </summary>
/// <remarks>
/// Precedence when multiple sources may be observed together: caller cancellation, then operation-level deadline/budget,
/// then per-attempt timeout scope, otherwise unknown cancellation. Uses token state only (no exception message inspection).
/// </remarks>
internal static class OperationCancellationClassifier
{
    /// <summary>
    /// Classifies linked cancellation sources using explicit token state.
    /// </summary>
    /// <param name="callerCanceled">True when the outer caller token is canceled.</param>
    /// <param name="operationEffectiveCanceled">True when the operation-level effective token is canceled.</param>
    /// <param name="perAttemptScopeCanceled">True when the per-attempt linked source is canceled.</param>
    /// <returns>The highest-precedence scenario matching the inputs.</returns>
    public static CancellationScenarioKind ClassifyFromLinkedTokenState(bool callerCanceled, bool operationEffectiveCanceled, bool perAttemptScopeCanceled) =>
        (callerCanceled, operationEffectiveCanceled, perAttemptScopeCanceled) switch
        {
            (true, _, _) => CancellationScenarioKind.CallerCanceled,
            (_, true, _) => CancellationScenarioKind.OperationDeadlineExceeded,
            (_, _, true) => CancellationScenarioKind.PerAttemptTimedOut,
            _ => CancellationScenarioKind.UnknownCancellation,
        };

    /// <summary>
    /// Classifies cancellation for the logical pipeline deadline decorator: caller token plus linked <c>CancelAfter</c> budget token.
    /// </summary>
    /// <param name="callerToken">The outer caller cancellation token.</param>
    /// <param name="linkedPipelineToken">The token passed to the inner pipeline (caller linked with the deadline timer).</param>
    /// <returns>The canonical scenario for this two-token layout.</returns>
    public static CancellationScenarioKind ClassifyLogicalPipelineDeadlineCancellation(CancellationToken callerToken, CancellationToken linkedPipelineToken) =>
        ClassifyFromLinkedTokenState(callerToken.IsCancellationRequested, linkedPipelineToken.IsCancellationRequested, false);

    /// <summary>
    /// Classifies cancellation for a peer call attempt: caller, operation-effective budget token, and per-attempt composite token.
    /// </summary>
    /// <param name="callerToken">The application caller token.</param>
    /// <param name="operationEffectiveToken">Caller linked with optional ambient RPC deadline budget.</param>
    /// <param name="perAttemptCompositeToken">Token from the attempt's linked source (caller plus per-attempt timeout).</param>
    /// <returns>The canonical scenario for retry classification.</returns>
    public static CancellationScenarioKind ClassifyPeerCallAttemptCancellation(
        CancellationToken callerToken,
        CancellationToken operationEffectiveToken,
        CancellationToken perAttemptCompositeToken) => ClassifyFromLinkedTokenState(
        callerToken.IsCancellationRequested,
        operationEffectiveToken.IsCancellationRequested,
        perAttemptCompositeToken.IsCancellationRequested);

    /// <summary>
    /// Returns true when a gRPC <see cref="StatusCode.Cancelled" /> fault should be surfaced as caller cancellation.
    /// </summary>
    /// <param name="ex">The transport exception.</param>
    /// <param name="cancellationToken">The logical caller token for the operation.</param>
    /// <returns><see langword="true" /> when <paramref name="ex" /> is canceled and <paramref name="cancellationToken" /> is canceled; otherwise <see langword="false" />.</returns>
    public static bool IsCallerInitiatedGrpcCancellation(RpcException ex, CancellationToken cancellationToken) =>
        ex.StatusCode == StatusCode.Cancelled && cancellationToken.IsCancellationRequested;

    /// <summary>
    /// Returns true when a gRPC server-streaming read fault should be treated as cooperative watch shutdown.
    /// </summary>
    /// <param name="ex">The transport exception.</param>
    /// <param name="cancellationToken">The logical caller token for the watch consumer.</param>
    /// <returns><see langword="true" /> when <paramref name="cancellationToken" /> is canceled and <paramref name="ex" /> is a known abort status; otherwise <see langword="false" />.</returns>
    public static bool IsCallerInitiatedGrpcWatchStreamFault(RpcException ex, CancellationToken cancellationToken) =>
        cancellationToken.IsCancellationRequested && ex.StatusCode is StatusCode.Cancelled or StatusCode.Unavailable;

    /// <summary>
    /// Returns true while the linked operation effective token (caller plus optional RPC deadline budget) is not canceled,
    /// so per-attempt retry loops and transport-level retries may continue.
    /// </summary>
    /// <param name="operationEffectiveToken">Token linked from the caller and optional ambient deadline budget.</param>
    /// <returns><see langword="true" /> when <paramref name="operationEffectiveToken" /> is not canceled; otherwise <see langword="false" />.</returns>
    public static bool OperationEffectiveTokenAllowsRetryAttempt(CancellationToken operationEffectiveToken) => !operationEffectiveToken.IsCancellationRequested;
}
