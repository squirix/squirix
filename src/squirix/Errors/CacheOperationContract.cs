using System;
using System.Globalization;

namespace Squirix.Errors;

internal static class CacheOperationContract
{
    internal const string CounterOverflowDetail = "Overflow.";

    private const string InsertVersionMustExceedCurrentMessagePrefix = "Version must be greater than current (current=";

    /// <summary>
    /// Builds the stable <see cref="InvalidOperationException" /> message for rejecting an explicit insert version that is not greater than the current entry version.
    /// </summary>
    /// <param name="currentVersion">The current stored version.</param>
    /// <param name="providedVersion">The caller-provided explicit version.</param>
    /// <returns>The message text shared by local mutation and the gRPC contract detail.</returns>
    public static string InsertVersionMustExceedCurrentMessage(long currentVersion, long providedVersion) =>
        string.Create(CultureInfo.InvariantCulture, $"{InsertVersionMustExceedCurrentMessagePrefix}{currentVersion}, provided={providedVersion})");

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
}
