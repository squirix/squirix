using System;
using System.Globalization;
using Squirix.Server.Core;
using Squirix.Server.Node.Services;

namespace Squirix.Server.Errors;

internal static class CacheOperationContract
{
    private const string InsertVersionMustExceedCurrentMessagePrefix = "Version must be greater than current (current=";

    public static SquirixException InvalidCacheKey(string? key) => CacheKeyValidator.ToContractException(key);

    public static SquirixException MemoryPressure() => new(SquirixErrorCode.MemoryPressure, "MemoryPressure", ResourceExhaustedException.StableDetail);

    public static SquirixException NotFound() => new(SquirixErrorCode.NotFound, "NotFound", "Not found.");

    public static SquirixException OperationIdRequired() => new(SquirixErrorCode.OperationIdRequired, "OperationIdRequired", RpcMutationContracts.OperationIdRequiredDetail);

    public static SquirixException OperationIdInvalidFormat() =>
        new(SquirixErrorCode.OperationIdInvalidFormat, "OperationIdInvalidFormat", RpcMutationContracts.OperationIdInvalidFormatDetail);

    public static SquirixException OperationIdTooLong() =>
        new(SquirixErrorCode.OperationIdTooLong, "OperationIdTooLong", RpcMutationContracts.OperationIdTooLongDetail);

    public static SquirixException OperationIdReuseMismatch() => new(SquirixErrorCode.OperationIdReuseMismatch, "OperationIdReuseMismatch", OperationIdReuseMismatchException.StableDetail);

    public static SquirixException PayloadTooLarge(int maxBytes) => new(
        SquirixErrorCode.PayloadTooLarge,
        "PayloadTooLarge",
        $"Payload size limit is {maxBytes.ToString(CultureInfo.InvariantCulture)} bytes.");

    public static SquirixException TooManyRequests(string reason) => new(SquirixErrorCode.TooManyRequests, "TooManyRequests", $"Server is overloaded ({reason}).");

    /// <summary>
    /// Determines whether <paramref name="message" /> matches the insert explicit-version precondition message shape.
    /// </summary>
    /// <param name="message">An exception or RPC status detail string.</param>
    /// <returns><see langword="true" /> when <paramref name="message" /> identifies an insert version downgrade.</returns>
    internal static bool IsInsertVersionMustExceedCurrentMessage(string? message) => !string.IsNullOrEmpty(message) &&
                                                                                     message.StartsWith(InsertVersionMustExceedCurrentMessagePrefix, StringComparison.Ordinal) &&
                                                                                     message.Contains(", provided=", StringComparison.Ordinal);

    /// <summary>
    /// Determines whether <paramref name="message" /> matches the required operation-id contract.
    /// </summary>
    /// <param name="message">An exception or RPC status detail string.</param>
    /// <returns><see langword="true" /> when <paramref name="message" /> identifies a missing operation id.</returns>
    internal static bool IsOperationIdRequiredMessage(string? message) => string.Equals(message, RpcMutationContracts.OperationIdRequiredDetail, StringComparison.Ordinal);

    internal static bool IsOperationIdInvalidFormatMessage(string? message) => string.Equals(message, RpcMutationContracts.OperationIdInvalidFormatDetail, StringComparison.Ordinal);

    internal static bool IsOperationIdTooLongMessage(string? message) => string.Equals(message, RpcMutationContracts.OperationIdTooLongDetail, StringComparison.Ordinal);

    internal static bool IsOperationIdReuseMismatchMessage(string? message) => string.Equals(message, OperationIdReuseMismatchException.StableDetail, StringComparison.Ordinal);
}
