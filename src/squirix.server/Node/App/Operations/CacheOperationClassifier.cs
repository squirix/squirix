using System;
using Grpc.Core;
using Squirix.Server.Errors;

namespace Squirix.Server.Node.App.Operations;

/// <summary>
/// Maps domain results and common transport exceptions to <see cref="CacheOperationResults" /> labels for logical cache operation semantics.
/// </summary>
internal static class CacheOperationClassifier
{
    internal static string ClassifyCacheRemoveResult<T>(CacheRemoveResult<T> result) => result.Removed ? CacheOperationResults.Ok : CacheOperationResults.NotFound;

    internal static string ClassifyCacheValueResult<T>(NodeCacheValueResult<T> result) => result.Found ? CacheOperationResults.Ok : CacheOperationResults.NotFound;

    internal static string ClassifyException(Exception exception) => exception switch
    {
        TimeoutException => CacheOperationResults.DeadlineExceeded,
        OperationCanceledException => CacheOperationResults.Canceled,
        ResourceExhaustedException => CacheOperationResults.ResourceExhausted,
        RpcException { StatusCode: StatusCode.Cancelled } => CacheOperationResults.Canceled,
        RpcException { StatusCode: StatusCode.DeadlineExceeded } => CacheOperationResults.DeadlineExceeded,
        RpcException { StatusCode: StatusCode.ResourceExhausted } => CacheOperationResults.ResourceExhausted,
        ArgumentException => CacheOperationResults.InvalidArgument,
        _ => CacheOperationResults.Failed,
    };

    internal static string ClassifyFoundBool(bool found) => found ? CacheOperationResults.Ok : CacheOperationResults.NotFound;

    internal static string ClassifyNullableReferenceResult<T>(NodeCacheEntry<T>? result) => result is null ? CacheOperationResults.NotFound : CacheOperationResults.Ok;
}
