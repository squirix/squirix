using System;
using Grpc.Core;

namespace Squirix.Server.Errors;

internal static class ServerOpContract
{
    internal const string CommitOutcomeUnknownDetail = "COMMIT_OUTCOME_UNKNOWN";

    internal const string NoWriteMajorityDetail = "Replica group has no verified write majority; nothing was written.";

    internal const string RingFencedDetail =
        "Cache operations are refused: the key owner or this node detected a cluster ring mismatch with a peer; make the peer lists agree and restart the affected nodes.";

    internal const string RingMismatchDetail = "Cluster ring mismatch: the forwarding node and the key owner disagree on the peer list, ring settings or server version; nothing was executed.";

    private const string EntryTagCountExceededDetail = "Entry tag count exceeds the maximum of 32.";

    private const string EntryTagKeyTooLargeDetail = "Entry tag key exceeds the maximum UTF-8 size of 256 bytes.";

    private const string EntryTagValueTooLargeDetail = "Entry tag value exceeds the maximum UTF-8 size of 1024 bytes.";

    private const string InsertVersionMustExceedCurrentPrefix = "Version must be greater than current (current=";

    private const string PayloadTooLargeDetail = "Payload size limit is 4194304 bytes.";

    /// <summary>Creates the refusal of a replicated write before its local append, while the group has no verified write majority.</summary>
    /// <returns><see cref="StatusCode.Unavailable" /> without the unknown-outcome code: nothing was written, so a retry may run elsewhere or later.</returns>
    internal static RpcException NoWriteMajority() => new(new Status(StatusCode.Unavailable, NoWriteMajorityDetail));

    internal static SquirixException CommitOutcomeUnknown() => new(SquirixErrorCode.CommitOutcomeUnknown, "CommitOutcomeUnknown", CommitOutcomeUnknownDetail);

    internal static SquirixException EntryTagCountExceeded() => new(SquirixErrorCode.InvalidEntryTags, "InvalidEntryTags", EntryTagCountExceededDetail);

    internal static SquirixException EntryTagKeyTooLarge() => new(SquirixErrorCode.InvalidEntryTags, "InvalidEntryTags", EntryTagKeyTooLargeDetail);

    internal static SquirixException EntryTagValueTooLarge() => new(SquirixErrorCode.InvalidEntryTags, "InvalidEntryTags", EntryTagValueTooLargeDetail);

    internal static SquirixException InvalidCacheKey(string detail) => new(SquirixErrorCode.InvalidCacheKey, "InvalidCacheKey", detail);

    /// <summary>Determines whether <paramref name="message" /> matches the insert explicit-version precondition message shape.</summary>
    /// <param name="message">An exception or RPC status detail string.</param>
    /// <returns><see langword="true" /> when <paramref name="message" /> identifies an insert version downgrade.</returns>
    internal static bool IsInsertVersionMustExceedCurrentMessage(string? message) => !string.IsNullOrEmpty(message) &&
                                                                                     message.StartsWith(InsertVersionMustExceedCurrentPrefix, StringComparison.Ordinal) &&
                                                                                     message.Contains(", provided=", StringComparison.Ordinal);

    internal static bool IsOperationIdInvalidFormatMessage(string? message) =>
        string.Equals(message, RpcMutationContracts.OperationIdInvalidFormatDetail, StringComparison.Ordinal);

    /// <summary>Determines whether <paramref name="message" /> matches the required operation-id contract.</summary>
    /// <param name="message">An exception or RPC status detail string.</param>
    /// <returns><see langword="true" /> when <paramref name="message" /> identifies a missing operation id.</returns>
    internal static bool IsOperationIdRequiredMessage(string? message) => string.Equals(message, RpcMutationContracts.OperationIdRequiredDetail, StringComparison.Ordinal);

    internal static bool IsOperationIdReuseMismatchMessage(string? message) => string.Equals(message, ServerOpIdMismatchException.StableDetail, StringComparison.Ordinal);

    internal static bool IsOperationIdTooLongMessage(string? message) => string.Equals(message, RpcMutationContracts.OperationIdTooLongDetail, StringComparison.Ordinal);

    internal static SquirixException JournalDiskQuota() => new(SquirixErrorCode.JournalDiskQuota, "JournalDiskQuota", JournalCapacityExceededException.StableDetail);

    internal static SquirixException MemoryPressure() => new(SquirixErrorCode.MemoryPressure, "MemoryPressure", ResourceExhaustedException.StableDetail);

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

    internal static SquirixException PayloadTooLarge() => new(SquirixErrorCode.PayloadTooLarge, "PayloadTooLarge", PayloadTooLargeDetail);

    internal static SquirixException TooManyRequests(string reason) => new(SquirixErrorCode.TooManyRequests, "TooManyRequests", $"Server is overloaded ({reason}).");
}
