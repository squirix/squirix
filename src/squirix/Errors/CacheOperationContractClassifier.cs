using System;
using Grpc.Core;

namespace Squirix.Errors;

/// <summary>Deterministic classification helpers shared by transport mappers; does not perform HTTP or gRPC result mapping.</summary>
internal static class CacheOperationContractClassifier
{
    internal static bool IsOperationIdReuseMismatchDetail(string? detail) =>
        ClassifyFailedPreconditionDetail(detail) is CacheOperationFailedPreconditionKind.OperationIdReuseMismatch;

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
    private static CacheOperationFailedPreconditionKind ClassifyFailedPreconditionDetail(string? detail)
    {
        if (CacheOperationContract.IsCounterIncrementTypeMismatchRpcDetail(detail))
            return CacheOperationFailedPreconditionKind.CounterIncrementTypeMismatch;

        if (CacheOperationContract.IsInsertVersionMustExceedCurrentMessage(detail))
            return CacheOperationFailedPreconditionKind.InsertVersionMustExceedCurrent;

        if (CacheOperationContract.IsOperationIdReuseMismatchMessage(detail))
            return CacheOperationFailedPreconditionKind.OperationIdReuseMismatch;

        return CacheOperationFailedPreconditionKind.None;
    }
}
