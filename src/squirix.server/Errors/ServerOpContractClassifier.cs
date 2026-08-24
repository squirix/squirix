using System;
using Grpc.Core;

namespace Squirix.Server.Errors;

/// <summary>Deterministic classification helpers shared by transport mappers; does not perform HTTP or gRPC result mapping.</summary>
internal static class ServerOpContractClassifier
{
    /// <summary>
    /// When <paramref name="detail" /> matches the operation-id reuse mismatch contract, returns <see langword="true" />.
    /// </summary>
    /// <param name="detail">The gRPC status detail string.</param>
    /// <returns><see langword="true" /> when <paramref name="detail" /> matches the stable reuse mismatch contract.</returns>
    internal static bool IsOperationIdReuseMismatchDetail(string? detail) => ClassifyFailedPreconditionDetail(detail) is ServerFailedPreconditionKind.OperationIdReuseMismatch;

    /// <summary>
    /// When <paramref name="detail" /> matches a stable FailedPrecondition contract, exposes the string used as
    /// the <see cref="InvalidOperationException" /> message (the detail string itself).
    /// </summary>
    /// <param name="detail">The gRPC status detail string.</param>
    /// <param name="message">The invalid-operation message when the method returns <see langword="true" />.</param>
    /// <returns><see langword="true" /> when <paramref name="detail" /> matches insert-version precondition contracts.</returns>
    internal static bool TryGetFailedPreconditionMessage(string? detail, out string? message)
    {
        var kind = ClassifyFailedPreconditionDetail(detail);
        if (kind is ServerFailedPreconditionKind.InsertVersionMustExceedCurrent)
        {
            message = detail;
            return true;
        }

        message = null;
        return false;
    }

    /// <summary>
    /// Classifies <see cref="StatusCode.FailedPrecondition" /> status detail strings that map to a stable
    /// <see cref="InvalidOperationException" /> in the logical cache pipeline.
    /// </summary>
    /// <param name="detail">The gRPC status detail string.</param>
    /// <returns>The classified contract kind; <see cref="ServerFailedPreconditionKind.None" /> when no stable contract matches.</returns>
    private static ServerFailedPreconditionKind ClassifyFailedPreconditionDetail(string? detail)
    {
        if (ServerOpContract.IsInsertVersionMustExceedCurrentMessage(detail))
            return ServerFailedPreconditionKind.InsertVersionMustExceedCurrent;

        return ServerOpContract.IsOperationIdReuseMismatchMessage(detail) ? ServerFailedPreconditionKind.OperationIdReuseMismatch : ServerFailedPreconditionKind.None;
    }
}
