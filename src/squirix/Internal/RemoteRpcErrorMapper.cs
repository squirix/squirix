using System;
using System.Diagnostics.CodeAnalysis;
using System.Runtime.ExceptionServices;
using Grpc.Core;
using Squirix.Errors;

namespace Squirix.Internal;

/// <summary>
/// Maps stable remote cache RPC faults to typed public SDK exceptions.
/// </summary>
internal static class RemoteRpcErrorMapper
{
    /// <summary>
    /// Applies remote RPC error mapping and always throws (never returns normally).
    /// </summary>
    /// <param name="ex">The gRPC transport exception from the remote cache pipeline.</param>
    /// <exception cref="OperationIdRequiredException">When the server rejected a missing operation id.</exception>
    /// <exception cref="OperationIdReuseMismatchException">When the server rejected an operation-id reuse mismatch.</exception>
    /// <exception cref="RpcException">When no mapping applies; rethrows <paramref name="ex" /> with preserved stack.</exception>
    [DoesNotReturn]
    public static void Map(RpcException ex)
    {
        ArgumentNullException.ThrowIfNull(ex);

        if (ex.StatusCode == StatusCode.InvalidArgument && CacheOperationContract.IsOperationIdRequiredMessage(ex.Status.Detail))
            throw new OperationIdRequiredException(ex.Status.Detail, ex);

        if (ex.StatusCode == StatusCode.FailedPrecondition && CacheOperationContractClassifier.IsOperationIdReuseMismatchDetail(ex.Status.Detail))
            throw new OperationIdReuseMismatchException(ex.Status.Detail, ex);

        ExceptionDispatchInfo.Capture(ex).Throw();
    }
}
