using System;
using System.Globalization;

namespace Squirix.Server.Errors;

internal static class ServerOpContract
{
    private const string InsertVersionMustExceedCurrentMessagePrefix = "Version must be greater than current (current=";

    internal static SquirixException EntryTagCountExceeded(int maxCount) => new(
        SquirixErrorCode.InvalidEntryTags,
        "InvalidEntryTags",
        $"Entry tag count exceeds the maximum of {maxCount.ToString(CultureInfo.InvariantCulture)}.");

    internal static SquirixException EntryTagKeyTooLarge(int maxUtf8Bytes) => new(
        SquirixErrorCode.InvalidEntryTags,
        "InvalidEntryTags",
        $"Entry tag key exceeds the maximum UTF-8 size of {maxUtf8Bytes.ToString(CultureInfo.InvariantCulture)} bytes.");

    internal static SquirixException EntryTagValueTooLarge(int maxUtf8Bytes) => new(
        SquirixErrorCode.InvalidEntryTags,
        "InvalidEntryTags",
        $"Entry tag value exceeds the maximum UTF-8 size of {maxUtf8Bytes.ToString(CultureInfo.InvariantCulture)} bytes.");

    internal static SquirixException MemoryPressure() => new(SquirixErrorCode.MemoryPressure, "MemoryPressure", ResourceExhaustedException.StableDetail);

    internal static SquirixException JournalDiskQuota() => new(SquirixErrorCode.JournalDiskQuota, "JournalDiskQuota", JournalCapacityExceededException.StableDetail);

    internal static SquirixException OperationIdInvalidFormat() => new(
        SquirixErrorCode.OperationIdInvalidFormat,
        "OperationIdInvalidFormat",
        RpcMutationContracts.OperationIdInvalidFormatDetail);

    internal static SquirixException OperationIdRequired() => new(SquirixErrorCode.OperationIdRequired, "OperationIdRequired", RpcMutationContracts.OperationIdRequiredDetail);

    internal static SquirixException OperationIdReuseMismatch() => new(
        SquirixErrorCode.OperationIdReuseMismatch,
        "OperationIdReuseMismatch",
        ServerOpIdMismatchException.StableDetail);

    internal static SquirixException OperationIdTooLong() => new(SquirixErrorCode.OperationIdTooLong, "OperationIdTooLong", RpcMutationContracts.OperationIdTooLongDetail);

    internal static SquirixException PayloadTooLarge(int maxBytes) => new(
        SquirixErrorCode.PayloadTooLarge,
        "PayloadTooLarge",
        $"Payload size limit is {maxBytes.ToString(CultureInfo.InvariantCulture)} bytes.");

    internal static SquirixException TooManyRequests(string reason) => new(SquirixErrorCode.TooManyRequests, "TooManyRequests", $"Server is overloaded ({reason}).");

    internal static SquirixException InvalidCacheKey(string detail) => new(SquirixErrorCode.InvalidCacheKey, "InvalidCacheKey", detail);

    /// <summary>
    /// Determines whether <paramref name="message" /> matches the insert explicit-version precondition message shape.
    /// </summary>
    /// <param name="message">An exception or RPC status detail string.</param>
    /// <returns><see langword="true" /> when <paramref name="message" /> identifies an insert version downgrade.</returns>
    internal static bool IsInsertVersionMustExceedCurrentMessage(string? message) => !string.IsNullOrEmpty(message) &&
                                                                                     message.StartsWith(InsertVersionMustExceedCurrentMessagePrefix, StringComparison.Ordinal) &&
                                                                                     message.Contains(", provided=", StringComparison.Ordinal);

    internal static bool IsOperationIdInvalidFormatMessage(string? message) =>
        string.Equals(message, RpcMutationContracts.OperationIdInvalidFormatDetail, StringComparison.Ordinal);

    /// <summary>
    /// Determines whether <paramref name="message" /> matches the required operation-id contract.
    /// </summary>
    /// <param name="message">An exception or RPC status detail string.</param>
    /// <returns><see langword="true" /> when <paramref name="message" /> identifies a missing operation id.</returns>
    internal static bool IsOperationIdRequiredMessage(string? message) => string.Equals(message, RpcMutationContracts.OperationIdRequiredDetail, StringComparison.Ordinal);

    internal static bool IsOperationIdReuseMismatchMessage(string? message) => string.Equals(message, ServerOpIdMismatchException.StableDetail, StringComparison.Ordinal);

    internal static bool IsOperationIdTooLongMessage(string? message) => string.Equals(message, RpcMutationContracts.OperationIdTooLongDetail, StringComparison.Ordinal);
}
