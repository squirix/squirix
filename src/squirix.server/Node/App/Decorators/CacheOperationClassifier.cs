using System;
using Squirix.Server.Core;
using Squirix.Server.Errors;
using Squirix.Transport.Binary;

namespace Squirix.Server.Node.App.Decorators;

/// <summary>Maps domain results and common transport exceptions to <see cref="CacheOperationResults" /> labels for logical cache operation semantics.</summary>
internal static class CacheOperationClassifier
{
    internal static string ClassifyAddOrGetResult<T>(NodeCacheAddOrGetResult<T> result) =>
        result.Added || result.Existing.Found ? CacheOperationResults.Ok : CacheOperationResults.NotFound;

    internal static string ClassifyCacheValueResult<T>(NodeCacheValueResult<T> result) => result.Found ? CacheOperationResults.Ok : CacheOperationResults.NotFound;

    internal static string ClassifyException(Exception exception) => exception switch
    {
        TimeoutException => CacheOperationResults.DeadlineExceeded,
        OperationCanceledException => CacheOperationResults.Canceled,
        ResourceExhaustedException => CacheOperationResults.ResourceExhausted,
        JournalCapacityExceededException => CacheOperationResults.ResourceExhausted,
        ServerCallException { StatusCode: FrameStatus.Cancelled } => CacheOperationResults.Canceled,
        ServerCallException { StatusCode: FrameStatus.DeadlineExceeded } => CacheOperationResults.DeadlineExceeded,
        ServerCallException { StatusCode: FrameStatus.ResourceExhausted } => CacheOperationResults.ResourceExhausted,
        ArgumentException => CacheOperationResults.InvalidArgument,
        _ => CacheOperationResults.Failed,
    };

    /// <summary>Returns whether the decorators label <paramref name="exception" /> with a result; other exceptions pass through unlabeled.</summary>
    /// <param name="exception">The exception thrown by the inner cache.</param>
    /// <returns><see langword="true" /> when the exception gets a result label.</returns>
    internal static bool IsClassifiedException(Exception exception) =>
        exception is TimeoutException or OperationCanceledException or ResourceExhaustedException or JournalCapacityExceededException or ServerCallException or ArgumentException;

    internal static string ClassifyFoundBool(bool found) => found ? CacheOperationResults.Ok : CacheOperationResults.NotFound;

    internal static string ClassifyNullableReferenceResult<T>(NodeCacheEntry<T>? result) => result == null ? CacheOperationResults.NotFound : CacheOperationResults.Ok;
}
