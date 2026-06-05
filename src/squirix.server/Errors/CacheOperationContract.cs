using System;
using Squirix.Server.Core;

namespace Squirix.Server.Errors;

internal static class CacheOperationContract
{
    private const string InsertVersionMustExceedCurrentMessagePrefix = "Version must be greater than current (current=";

    public static SquirixException InvalidCacheKey(string? key) => CacheKeyValidator.ToContractException(key);

    public static SquirixException MemoryPressure() => new(SquirixErrorCode.MemoryPressure, "MemoryPressure", ResourceExhaustedException.StableDetail);

    public static SquirixException NotFound() => new(SquirixErrorCode.NotFound, "NotFound", "Not found.");

    public static SquirixException PayloadTooLarge(int maxBytes) => new(SquirixErrorCode.PayloadTooLarge, "PayloadTooLarge", $"Payload size limit is {maxBytes} bytes.");

    public static SquirixException TooManyRequests(string reason) => new(SquirixErrorCode.TooManyRequests, "TooManyRequests", $"Server is overloaded ({reason}).");

    /// <summary>
    /// Determines whether <paramref name="message" /> matches the insert explicit-version precondition message shape.
    /// </summary>
    /// <param name="message">An exception or RPC status detail string.</param>
    /// <returns><see langword="true" /> when <paramref name="message" /> identifies an insert version downgrade.</returns>
    internal static bool IsInsertVersionMustExceedCurrentMessage(string? message) => !string.IsNullOrEmpty(message) &&
                                                                                     message.StartsWith(InsertVersionMustExceedCurrentMessagePrefix, StringComparison.Ordinal) &&
                                                                                     message.Contains(", provided=", StringComparison.Ordinal);
}
