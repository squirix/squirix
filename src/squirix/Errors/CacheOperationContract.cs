using System;

namespace Squirix.Errors;

internal static class CacheOperationContract
{
    private const string InsertVersionMustExceedCurrentMessagePrefix = "Version must be greater than current (current=";

    /// <summary>
    /// Determines whether <paramref name="detail" /> matches the stable increment counter type-mismatch contract (FailedPrecondition),
    /// distinct from CAS <c>Version mismatch</c> and routing <c>StaleOwner</c> texts.
    /// </summary>
    /// <param name="detail">The gRPC status detail string.</param>
    /// <returns><see langword="true" /> when <paramref name="detail" /> identifies a counter increment type mismatch.</returns>
    internal static bool IsCounterIncrementTypeMismatchRpcDetail(string? detail) => !string.IsNullOrWhiteSpace(detail) &&
                                                                                    detail.Contains("Type mismatch", StringComparison.OrdinalIgnoreCase) && detail.Contains(
                                                                                        "expected",
                                                                                        StringComparison.OrdinalIgnoreCase);

    /// <summary>
    /// Determines whether <paramref name="message" /> matches the insert explicit-version precondition message shape.
    /// </summary>
    /// <param name="message">An exception or RPC status detail string.</param>
    /// <returns><see langword="true" /> when <paramref name="message" /> identifies an insert version downgrade.</returns>
    internal static bool IsInsertVersionMustExceedCurrentMessage(string? message) => !string.IsNullOrEmpty(message) &&
                                                                                     message.StartsWith(InsertVersionMustExceedCurrentMessagePrefix, StringComparison.Ordinal) &&
                                                                                     message.Contains(", provided=", StringComparison.Ordinal);

    internal static bool IsOperationIdRequiredMessage(string? message) => string.Equals(message, OperationIdRequiredException.StableDetail, StringComparison.Ordinal);

    internal static bool IsOperationIdReuseMismatchMessage(string? message) => string.Equals(message, OperationIdReuseMismatchException.StableDetail, StringComparison.Ordinal);
}
