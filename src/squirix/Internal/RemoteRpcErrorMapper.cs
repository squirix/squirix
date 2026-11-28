using System;
using System.Diagnostics.CodeAnalysis;
using System.Runtime.ExceptionServices;
using Squirix.Transport;

namespace Squirix.Internal;

internal static class RemoteRpcErrorMapper
{
    /// <summary>Applies remote RPC error mapping and always throws (never returns normally).</summary>
    /// <param name="ex">The transport exception from the remote cache pipeline.</param>
    /// <exception cref="OperationIdRequiredException">When the server rejected a missing operation id.</exception>
    /// <exception cref="OperationIdReuseMismatchException">When the server rejected an operation-id reuse mismatch.</exception>
    /// <exception cref="CommitOutcomeUnknownException">When the server reports an ambiguous durable commit outcome.</exception>
    /// <exception cref="SquirixTransportException">When no mapping applies; rethrows <paramref name="ex" /> with preserved stack.</exception>
    [DoesNotReturn]
    internal static void Map(SquirixTransportException ex)
    {
        ArgumentNullException.ThrowIfNull(ex);

        if (ex.StatusCode is CacheTransportStatusCode.InvalidArgument && CacheOperationContract.IsOperationIdRequiredMessage(ex.Status.Detail))
            throw new OperationIdRequiredException(ex.Status.Detail, ex);

        if (ex.StatusCode is CacheTransportStatusCode.FailedPrecondition && CacheOperationContract.IsOperationIdReuseMismatchMessage(ex.Status.Detail))
            throw new OperationIdReuseMismatchException(ex.Status.Detail, ex);

        var commitUnknown = CommitOutcomeUnknownClassifier.Map(ex);
        if (commitUnknown != null)
            throw commitUnknown;

        ExceptionDispatchInfo.Capture(ex).Throw();
    }

    private static class CacheOperationContract
    {
        internal static bool IsOperationIdRequiredMessage(string? message) => string.Equals(message, OperationIdRequiredException.StableDetail, StringComparison.Ordinal);

        internal static bool IsOperationIdReuseMismatchMessage(string? message) =>
            string.Equals(message, OperationIdReuseMismatchException.StableDetail, StringComparison.Ordinal);
    }
}
