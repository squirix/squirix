using System;
using Grpc.Core;

namespace Squirix.Errors;

/// <summary>
/// Deterministic classification helpers shared by transport mappers; does not perform HTTP or gRPC result mapping.
/// </summary>
internal static class CacheOperationContractClassifier
{
    /// <summary>
    /// Classifies <see cref="StatusCode.FailedPrecondition" /> status detail strings that map to a stable
    /// <see cref="InvalidOperationException" /> in the logical cache pipeline.
    /// </summary>
    /// <param name="detail">The gRPC status detail string.</param>
    /// <returns>The classified contract kind; <see cref="CacheOperationFailedPreconditionKind.None" /> when no stable contract matches.</returns>
    /// <remarks>
    /// Classification order matches the domain transport error mapper historical behavior:
    /// counter increment type mismatch is evaluated before insert-version precondition text.
    /// </remarks>
    public static CacheOperationFailedPreconditionKind ClassifyFailedPreconditionDetail(string? detail)
    {
        return CacheOperationContract.IsCounterIncrementTypeMismatchRpcDetail(detail) ? CacheOperationFailedPreconditionKind.CounterIncrementTypeMismatch :
            CacheOperationContract.IsInsertVersionMustExceedCurrentMessage(detail) ? CacheOperationFailedPreconditionKind.InsertVersionMustExceedCurrent :
            CacheOperationFailedPreconditionKind.None;
    }

    /// <summary>
    /// Determines whether the transport fault matches the stable counter overflow contract
    /// (<see cref="StatusCode.InvalidArgument" /> with <see cref="CacheOperationContract.CounterOverflowDetail" />).
    /// </summary>
    /// <param name="statusCode">The gRPC status code from the transport fault.</param>
    /// <param name="detail">The gRPC status detail string.</param>
    /// <returns><see langword="true" /> when the fault should map to <see cref="OverflowException" /> in the domain pipeline.</returns>
    public static bool IsCounterOverflowRpcFault(StatusCode statusCode, string? detail) => statusCode == StatusCode.InvalidArgument &&
                                                                                           string.Equals(
                                                                                               detail,
                                                                                               CacheOperationContract.CounterOverflowDetail,
                                                                                               StringComparison.Ordinal);

    /// <summary>
    /// When <paramref name="detail" /> matches a stable FailedPrecondition contract, exposes the string used as
    /// the <see cref="InvalidOperationException" /> message (the detail string itself).
    /// </summary>
    /// <param name="detail">The gRPC status detail string.</param>
    /// <param name="message">The invalid-operation message when the method returns <see langword="true" />.</param>
    /// <returns><see langword="true" /> when <paramref name="detail" /> matches counter type mismatch or insert-version precondition contracts.</returns>
    public static bool TryGetFailedPreconditionInvalidOperationMessage(string? detail, out string message)
    {
        var kind = ClassifyFailedPreconditionDetail(detail);
        if (kind == CacheOperationFailedPreconditionKind.None)
        {
            message = null!;
            return false;
        }

        message = detail!;
        return true;
    }
}
