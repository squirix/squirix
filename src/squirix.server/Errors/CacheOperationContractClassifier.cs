using System;
using Grpc.Core;

namespace Squirix.Server.Errors;

/// <summary>
/// Deterministic classification helpers shared by transport mappers; does not perform HTTP or gRPC result mapping.
/// </summary>
internal static class CacheOperationContractClassifier
{
    /// <summary>
    /// When <paramref name="detail" /> matches a stable FailedPrecondition contract, exposes the string used as
    /// the <see cref="InvalidOperationException" /> message (the detail string itself).
    /// </summary>
    /// <param name="detail">The gRPC status detail string.</param>
    /// <param name="message">The invalid-operation message when the method returns <see langword="true" />.</param>
    /// <returns><see langword="true" /> when <paramref name="detail" /> matches insert-version precondition contracts.</returns>
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

    /// <summary>
    /// Classifies <see cref="StatusCode.FailedPrecondition" /> status detail strings that map to a stable
    /// <see cref="InvalidOperationException" /> in the logical cache pipeline.
    /// </summary>
    /// <param name="detail">The gRPC status detail string.</param>
    /// <returns>The classified contract kind; <see cref="CacheOperationFailedPreconditionKind.None" /> when no stable contract matches.</returns>
    private static CacheOperationFailedPreconditionKind ClassifyFailedPreconditionDetail(string? detail)
    {
        return CacheOperationContract.IsInsertVersionMustExceedCurrentMessage(detail)
            ? CacheOperationFailedPreconditionKind.InsertVersionMustExceedCurrent
            : CacheOperationFailedPreconditionKind.None;
    }
}
