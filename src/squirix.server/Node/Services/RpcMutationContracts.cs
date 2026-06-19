using Grpc.Core;
using Squirix.Server.Errors;

namespace Squirix.Server.Node.Services;

/// <summary>Shared validation and stable detail strings for mutating RPC idempotency.</summary>
internal static class RpcMutationContracts
{
    /// <summary>
    /// Stable detail for missing <c>operation_id</c> on mutating RPCs.
    /// </summary>
    public const string OperationIdRequiredDetail = "operation_id is required for mutating cache RPCs.";

    /// <summary>
    /// Stable detail for <c>operation_id</c> values that exceed <see cref="OperationIdLength" />.
    /// </summary>
    public const string OperationIdTooLongDetail = "operation_id exceeds the maximum length of 32 characters.";

    /// <summary>
    /// Stable detail for <c>operation_id</c> values that are not 32 lowercase hex characters.
    /// </summary>
    public const string OperationIdInvalidFormatDetail = "operation_id must be 32 lowercase hex characters (UUID without hyphens).";

    /// <summary>Maximum allowed length of <c>operation_id</c> on mutating RPCs.</summary>
    internal const int OperationIdLength = 32;

    /// <summary>Requires a non-empty operation identifier and returns the normalized value.</summary>
    /// <param name="operationId">The operation identifier from the transport request.</param>
    /// <returns>The normalized operation identifier.</returns>
    /// <exception cref="RpcException">When <paramref name="operationId" /> is missing, too long, or invalid.</exception>
    public static string RequireOperationId(string? operationId)
    {
        if (string.IsNullOrWhiteSpace(operationId))
            throw CacheOperationContract.OperationIdRequired().ToRpcException();

        if (operationId.Length > OperationIdLength)
            throw CacheOperationContract.OperationIdTooLong().ToRpcException();

        if (!IsLowercaseHexOperationId(operationId))
            throw CacheOperationContract.OperationIdInvalidFormat().ToRpcException();

        return operationId;
    }

    private static bool IsLowercaseHexOperationId(string operationId)
    {
        if (operationId.Length is not OperationIdLength)
            return false;

        foreach (var c in operationId)
        {
            if (c is >= '0' and <= '9' or >= 'a' and <= 'f')
                continue;

            return false;
        }

        return true;
    }
}
