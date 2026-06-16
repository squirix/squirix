using Grpc.Core;
using Squirix.Server.Errors;

namespace Squirix.Server.Node.Services;

/// <summary>
/// Shared validation and stable detail strings for mutating RPC idempotency.
/// </summary>
internal static class RpcMutationContracts
{
    /// <summary>
    /// Stable detail for missing <c>operation_id</c> on mutating RPCs.
    /// </summary>
    public const string OperationIdRequiredDetail = "operation_id is required for mutating cache RPCs.";

    /// <summary>
    /// Requires a non-empty operation identifier and returns the normalized value.
    /// </summary>
    /// <param name="operationId">The operation identifier from the transport request.</param>
    /// <returns>The normalized operation identifier.</returns>
    /// <exception cref="RpcException">When <paramref name="operationId" /> is missing or whitespace.</exception>
    public static string RequireOperationId(string? operationId)
    {
        if (string.IsNullOrWhiteSpace(operationId))
            throw CacheOperationContract.OperationIdRequired().ToRpcException();

        return operationId;
    }
}
