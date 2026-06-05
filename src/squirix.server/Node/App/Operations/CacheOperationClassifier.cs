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

    internal static string ClassifyCacheValueResult<T>(CacheValueResult<T> result) => result.Found ? CacheOperationResults.Ok : CacheOperationResults.NotFound;

    internal static string ClassifyException(Exception exception) => exception switch
    {
        TimeoutException => CacheOperationResults.DeadlineExceeded,
        OperationCanceledException => CacheOperationResults.Cancelled,
        ResourceExhaustedException => CacheOperationResults.ResourceExhausted,
        RpcException { StatusCode: StatusCode.Cancelled } => CacheOperationResults.Cancelled,
        RpcException { StatusCode: StatusCode.DeadlineExceeded } => CacheOperationResults.DeadlineExceeded,
        RpcException { StatusCode: StatusCode.ResourceExhausted } => CacheOperationResults.ResourceExhausted,
        ArgumentException => CacheOperationResults.InvalidArgument,
        _ => CacheOperationResults.Failed,
    };

    internal static string ClassifyFoundBool(bool found) => found ? CacheOperationResults.Ok : CacheOperationResults.NotFound;

    internal static string ClassifyNullableReferenceResult<T>(CacheEntry<T>? result) => result is null ? CacheOperationResults.NotFound : CacheOperationResults.Ok;

    internal static string ClassifyNullableValueResult(TimeSpan? result) => result.HasValue ? CacheOperationResults.Ok : CacheOperationResults.NotFound;
}
