using System;
using System.Diagnostics.CodeAnalysis;
using System.Runtime.ExceptionServices;
using System.Threading;
using Grpc.Core;
using Squirix.Server.Errors;
using Squirix.Server.Limits;

namespace Squirix.Server.Node.App.Decorators;

/// <summary>
/// Normalizes selected transport-level <see cref="RpcException" /> failures from the logical cache pipeline
/// (for example mapping caller cancellation to <see cref="OperationCanceledException" />, counter increment
/// overflow to <see cref="OverflowException" /> when the server uses the stable overflow contract detail,
/// increment counter type mismatch to <see cref="InvalidOperationException" /> for the stable type-mismatch detail,
/// and insert explicit version downgrade to <see cref="InvalidOperationException" /> for the stable insert version precondition detail).
/// </summary>
/// <remarks>
///     <para>
///     This mapper intentionally does not convert <see cref="StatusCode.Unknown" />,
///     and does not treat ambiguous outcomes as success.
///     </para>
///     <para>
///     Use <see cref="Map" /> instead of catching and rethrowing <see cref="RpcException" /> manually so original stacks
///     are preserved via <see cref="ExceptionDispatchInfo" /> when no domain wrapper applies.
///     </para>
/// </remarks>
internal static class DomainTransportErrorMapper
{
    /// <summary>
    /// Applies domain transport error mapping and always throws (never returns normally).
    /// </summary>
    /// <param name="ex">The gRPC transport exception from the inner pipeline.</param>
    /// <param name="cancellationToken">The caller cancellation token for the logical operation.</param>
    /// <remarks>
    ///     <para>
    ///     When a stable domain exception is introduced for a specific <see cref="RpcException" />, throw it with
    ///     <paramref name="ex" /> as <see cref="Exception.InnerException" /> so the original fault context is preserved.
    ///     </para>
    ///     <para>
    ///     When no mapping applies, the original <see cref="RpcException" /> is rethrown with its stack trace preserved.
    ///     </para>
    /// </remarks>
    /// <exception cref="OperationCanceledException">When <paramref name="ex" /> represents caller cancellation.</exception>
    /// <exception cref="OverflowException">When <paramref name="ex" /> represents the stable counter overflow contract.</exception>
    /// <exception cref="InvalidOperationException">
    /// When <paramref name="ex" /> represents the stable increment type-mismatch contract or the stable insert explicit-version precondition
    /// contract.
    /// </exception>
    /// <exception cref="RpcException">When no mapping applies; rethrows <paramref name="ex" /> with preserved stack.</exception>
    [DoesNotReturn]
    public static void Map(RpcException ex, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(ex);

        ThrowIfCallerCancellation(ex, cancellationToken);
        ThrowIfInvalidArgumentContract(ex);
        ThrowIfFailedPreconditionContract(ex);
        ThrowIfPayloadTooLargeContract(ex);
        RethrowOriginal(ex);
    }

    [DoesNotReturn]
    private static void RethrowOriginal(RpcException ex) => ExceptionDispatchInfo.Capture(ex).Throw();

    private static void ThrowIfCallerCancellation(RpcException ex, CancellationToken cancellationToken)
    {
        if (OperationCancellationClassifier.IsCallerInitiatedGrpcCancellation(ex, cancellationToken))
            cancellationToken.ThrowIfCancellationRequested();
    }

    private static void ThrowIfPayloadTooLargeContract(RpcException ex)
    {
        if (ex.StatusCode != StatusCode.ResourceExhausted)
            return;

        var detail = ex.Status.Detail;
        if (!detail.StartsWith("Payload size limit is ", StringComparison.Ordinal))
            return;

        throw CacheOperationContract.PayloadTooLarge(SquirixEntryLimits.MaxEntrySizeBytes);
    }

    private static void ThrowIfFailedPreconditionContract(RpcException ex)
    {
        if (ex.StatusCode != StatusCode.FailedPrecondition)
            return;

        if (CacheOperationContractClassifier.TryGetFailedPreconditionInvalidOperationMessage(ex.Status.Detail, out var message))
            throw new InvalidOperationException(message, ex);
    }

    private static void ThrowIfInvalidArgumentContract(RpcException ex)
    {
        if (ex.StatusCode != StatusCode.InvalidArgument)
            return;

        throw new ArgumentException(ex.Status.Detail, ex);
    }
}
