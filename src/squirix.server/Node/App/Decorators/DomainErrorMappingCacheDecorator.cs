using System;
using System.Diagnostics.CodeAnalysis;
using System.Runtime.CompilerServices;
using System.Runtime.ExceptionServices;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Attributes;
using Squirix.Server.Core;
using Squirix.Server.Errors;
using Squirix.Server.Node.Observability;
using Squirix.Server.Runtime.Contracts;
using Squirix.Transport.Binary;

namespace Squirix.Server.Node.App.Decorators;

/// <summary>Maps transport-level <see cref="ServerCallException" /> failures from clustered remote calls where a stable normalization exists.</summary>
/// <typeparam name="T">The cache value type.</typeparam>
[Immutable]
internal sealed class DomainErrorMappingCacheDecorator<T> : IRequestCache<T>
{
    private readonly ILogicalNamespacedCache<T> _inner;

    private readonly PressureGate? _pressure;

    internal DomainErrorMappingCacheDecorator(ILogicalNamespacedCache<T> inner, PressureGate? pressure = null)
    {
        ArgumentNullException.ThrowIfNull(inner);
        _inner = inner;
        _pressure = pressure;
    }

    public ValueTask<NodeCacheEntry<T>?> GetEntryAsync(string cacheName, string key, ICacheRequestContext? request, CancellationToken cancellationToken) => WithMappingAsync(
        static (inner, args, ct, context) => inner.GetEntryForRequestAsync(args.CacheName, args.Key, ct, context),
        new ReadKeyArgs(cacheName, key),
        request,
        cancellationToken);

    public ValueTask<NodeCacheValueRead<T>> GetValueWithDeadlineAsync(string cacheName, string key, ICacheRequestContext? request, CancellationToken cancellationToken) => WithMappingAsync(
        static (inner, args, ct, context) => inner.GetValueWithDeadlineForRequestAsync(args.CacheName, args.Key, ct, context),
        new ReadKeyArgs(cacheName, key),
        request,
        cancellationToken);

    public ValueTask<NodeCacheValueResult<T>> GetValueAsync(string cacheName, string key, ICacheRequestContext? request, CancellationToken cancellationToken) => WithMappingAsync(
        static (inner, args, ct, context) => inner.GetValueForRequestAsync(args.CacheName, args.Key, ct, context),
        new ReadKeyArgs(cacheName, key),
        request,
        cancellationToken);

    public ValueTask<bool> RemoveAsync(string operationId, string cacheName, string key, ICacheRequestContext? request, CancellationToken cancellationToken) => WithMappingAsync(
        static (inner, args, ct, context) => inner.RemoveForRequestAsync(args.OperationId, args.CacheName, args.Key, ct, context),
        new MutationKeyArgs(operationId, cacheName, key),
        request,
        cancellationToken);

    public ValueTask<bool> RemoveExpirationAsync(string operationId, string cacheName, string key, ICacheRequestContext? request, CancellationToken cancellationToken) => WithMappingAsync(
        static (inner, args, ct, context) => inner.RemoveExpirationForRequestAsync(args.OperationId, args.CacheName, args.Key, ct, context),
        new MutationKeyArgs(operationId, cacheName, key),
        request,
        cancellationToken);

    public ValueTask SetEntryAsync(string operationId, string cacheName, string key, NodeCacheEntry<T> entry, ICacheRequestContext? request, CancellationToken cancellationToken) => WithMappingAsync(
        static (inner, args, ct, context) => inner.SetEntryForRequestAsync(args.OperationId, args.CacheName, args.Key, args.Entry, ct, context),
        new SetEntryArgs<T>(operationId, cacheName, key, entry),
        request,
        cancellationToken);

    public ValueTask<bool> TouchAsync(string operationId, string cacheName, string key, TimeSpan expiration, ICacheRequestContext? request, CancellationToken cancellationToken) => WithMappingAsync(
        static (inner, args, ct, context) => inner.TouchForRequestAsync(args.OperationId, args.CacheName, args.Key, args.Expiration, ct, context),
        new TouchArgs(operationId, cacheName, key, expiration),
        request,
        cancellationToken);

    public ValueTask<bool> TryAddEntryAsync(string operationId, string cacheName, string key, NodeCacheEntry<T> entry, ICacheRequestContext? request, CancellationToken cancellationToken) => WithMappingAsync(
        static (inner, args, ct, context) => inner.TryAddEntryForRequestAsync(args.OperationId, args.CacheName, args.Key, args.Entry, ct, context),
        new SetEntryArgs<T>(operationId, cacheName, key, entry),
        request,
        cancellationToken);

    public ValueTask<bool> TryReplaceEntryAsync(string operationId, string cacheName, string key, NodeCacheEntry<T> entry, ICacheRequestContext? request, CancellationToken cancellationToken) => WithMappingAsync(
        static (inner, args, ct, context) => inner.TryReplaceEntryForRequestAsync(args.OperationId, args.CacheName, args.Key, args.Entry, ct, context),
        new SetEntryArgs<T>(operationId, cacheName, key, entry),
        request,
        cancellationToken);

    public ValueTask<NodeCacheAddOrGetResult<T>> AddOrGetAsync(string operationId, string cacheName, string key, NodeCacheEntry<T> entry, ICacheRequestContext? request, CancellationToken cancellationToken) =>
        WithMappingAsync(
            static (inner, args, ct, context) => inner.AddOrGetForRequestAsync(args.OperationId, args.CacheName, args.Key, args.Entry, ct, context),
            new SetEntryArgs<T>(operationId, cacheName, key, entry),
            request,
            cancellationToken);

    public ValueTask<bool> UpdateAsync(string operationId, string cacheName, string key, T? value, ICacheRequestContext? request, CancellationToken cancellationToken) => WithMappingAsync(
        static (inner, args, ct, context) => inner.UpdateForRequestAsync(args.OperationId, args.CacheName, args.Key, args.Value, ct, context),
        new UpdateArgs<T>(operationId, cacheName, key, value),
        request,
        cancellationToken);

    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder))]
    private async ValueTask WithMappingAsync<TState>(
        Func<ILogicalNamespacedCache<T>, TState, CancellationToken, ICacheRequestContext?, ValueTask> invoke,
        TState state,
        ICacheRequestContext? request,
        CancellationToken cancellationToken)
    {
        try
        {
            await invoke(_inner, state, cancellationToken, request).ConfigureAwait(false);
        }
        catch (ResourceExhaustedException rejection) when (rejection.WasCritical != null && _pressure != null)
        {
            _pressure.ReportLocalRejection(rejection);
            throw;
        }
        catch (ServerCallException ex)
        {
            DomainTransportErrorMapper.Map(ex, cancellationToken);
        }
    }

    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder<>))]
    private async ValueTask<TResult> WithMappingAsync<TState, TResult>(
        Func<ILogicalNamespacedCache<T>, TState, CancellationToken, ICacheRequestContext?, ValueTask<TResult>> invoke,
        TState state,
        ICacheRequestContext? request,
        CancellationToken cancellationToken)
    {
        try
        {
            return await invoke(_inner, state, cancellationToken, request).ConfigureAwait(false);
        }
        catch (ResourceExhaustedException rejection) when (rejection.WasCritical != null && _pressure != null)
        {
            _pressure.ReportLocalRejection(rejection);
            throw;
        }
        catch (ServerCallException ex)
        {
            DomainTransportErrorMapper.Map(ex, cancellationToken);
            return default;
        }
    }

    /// <summary>
    /// Normalizes selected transport-level <see cref="ServerCallException" /> failures from the logical cache pipeline
    /// (for example mapping caller cancellation to <see cref="OperationCanceledException" />, counter increment
    /// overflow to <see cref="OverflowException" /> when the server uses the stable overflow contract detail,
    /// increment counter type mismatch to <see cref="InvalidOperationException" /> for the stable type-mismatch detail,
    /// and insert explicit version downgrade to <see cref="InvalidOperationException" /> for the stable insert version precondition detail).
    /// </summary>
    /// <remarks>
    ///     <para>
    ///     This mapper intentionally does not convert <see cref="FrameStatus.Unknown" />,
    ///     and does not treat ambiguous outcomes as success.
    ///     </para>
    ///     <para>
    ///     Use <see cref="Map" /> instead of catching and rethrowing <see cref="ServerCallException" /> manually so original stacks
    ///     are preserved via <see cref="ExceptionDispatchInfo" /> when no domain wrapper applies.
    ///     </para>
    /// </remarks>
    private static class DomainTransportErrorMapper
    {
        /// <summary>Applies domain transport error mapping and always throws (never returns normally).</summary>
        /// <param name="ex">The gRPC transport exception from the inner pipeline.</param>
        /// <param name="cancellationToken">The caller cancellation token for the logical operation.</param>
        /// <remarks>
        ///     <para>
        ///     When a stable domain exception is introduced for a specific <see cref="ServerCallException" />, throw it with
        ///     <paramref name="ex" /> as <see cref="Exception.InnerException" /> so the original fault context is preserved.
        ///     </para>
        ///     <para>
        ///     When no mapping applies, the original <see cref="ServerCallException" /> is rethrown with its stack trace preserved.
        ///     </para>
        /// </remarks>
        /// <exception cref="OperationCanceledException">When <paramref name="ex" /> represents caller cancellation.</exception>
        /// <exception cref="OverflowException">When <paramref name="ex" /> represents the stable counter overflow contract.</exception>
        /// <exception cref="InvalidOperationException">
        /// When <paramref name="ex" /> represents the stable increment type-mismatch contract or the stable insert explicit-version precondition
        /// contract.
        /// </exception>
        /// <exception cref="ServerCallException">When no mapping applies; rethrows <paramref name="ex" /> with preserved stack.</exception>
        [DoesNotReturn]
        internal static void Map(ServerCallException ex, CancellationToken cancellationToken)
        {
            ArgumentNullException.ThrowIfNull(ex);

            ThrowIfCallerCancellation(ex, cancellationToken);
            ThrowIfInvalidArgumentContract(ex);
            ThrowIfFailedPreconditionContract(ex);
            ThrowIfPayloadTooLargeContract(ex);
            RethrowOriginal(ex);
        }

        [DoesNotReturn]
        private static void RethrowOriginal(ServerCallException ex) => ExceptionDispatchInfo.Capture(ex).Throw();

        private static void ThrowIfCallerCancellation(ServerCallException ex, CancellationToken cancellationToken)
        {
            if (ServerCancelClassifier.IsCallerCancellation(ex, cancellationToken))
                cancellationToken.ThrowIfCancellationRequested();
        }

        private static void ThrowIfFailedPreconditionContract(ServerCallException ex)
        {
            if (ex.StatusCode != FrameStatus.FailedPrecondition)
                return;

            if (ServerOpContractClassifier.TryGetFailedPreconditionMessage(ex.Message, out var message))
                throw new InvalidOperationException(message, ex);

            if (ServerOpContractClassifier.IsOperationIdReuseMismatchDetail(ex.Message))
                throw new ServerOpIdMismatchException(ex.Message, ex);
        }

        private static void ThrowIfInvalidArgumentContract(ServerCallException ex)
        {
            if (ex.StatusCode != FrameStatus.InvalidArgument)
                return;

            if (ServerOpContract.IsOperationIdRequiredMessage(ex.Message))
                throw ServerOpContract.OperationIdRequired();

            if (ServerOpContract.IsOperationIdInvalidFormatMessage(ex.Message))
                throw ServerOpContract.OperationIdInvalidFormat();

            if (ServerOpContract.IsOperationIdTooLongMessage(ex.Message))
                throw ServerOpContract.OperationIdTooLong();

            throw new ArgumentException(ex.Message, nameof(ex), ex);
        }

        private static void ThrowIfPayloadTooLargeContract(ServerCallException ex)
        {
            if (ex.StatusCode != FrameStatus.ResourceExhausted)
                return;

            var detail = ex.Message;
            if (!detail.StartsWith("Payload size limit is ", StringComparison.Ordinal))
                return;

            throw new SquirixException(SquirixErrorCode.PayloadTooLarge, "PayloadTooLarge", detail);
        }
    }
}
