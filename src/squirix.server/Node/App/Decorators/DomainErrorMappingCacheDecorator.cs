using System;
using System.Diagnostics.CodeAnalysis;
using System.Runtime.ExceptionServices;
using System.Threading;
using System.Threading.Tasks;
using Grpc.Core;
using Squirix.Server.Attributes;
using Squirix.Server.Core;
using Squirix.Server.Errors;
using Squirix.Server.Node.Observability;
using Squirix.Server.Runtime.Contracts;

namespace Squirix.Server.Node.App.Decorators;

/// <summary>
/// Maps transport-level <see cref="RpcException" /> failures from clustered remote calls where a stable normalization exists.
/// </summary>
/// <typeparam name="T">The cache value type.</typeparam>
[Immutable]
internal sealed class DomainErrorMappingCacheDecorator<T> : ILogicalNamespacedCache<T>
{
    private readonly ILogicalNamespacedCache<T> _inner;

    internal DomainErrorMappingCacheDecorator(ILogicalNamespacedCache<T> inner)
    {
        _inner = inner ?? throw new ArgumentNullException(nameof(inner));
    }

    public ValueTask<NodeCacheEntry<T>?> GetEntryAsync(string cacheName, string key, CancellationToken cancellationToken) => WithMappingAsync(
        static (inner, args, ct) => inner.GetEntryAsync(args.CacheName, args.Key, ct),
        new ReadKeyArgs(cacheName, key),
        cancellationToken);

    public ValueTask<NodeCacheValueResult<T>> GetValueAsync(string cacheName, string key, CancellationToken cancellationToken) => WithMappingAsync(
        static (inner, args, ct) => inner.GetValueAsync(args.CacheName, args.Key, ct),
        new ReadKeyArgs(cacheName, key),
        cancellationToken);

    public ValueTask<CacheRemoveResult<T>> RemoveAsync(string operationId, string cacheName, string key, CancellationToken cancellationToken) => WithMappingAsync(
        static (inner, args, ct) => inner.RemoveAsync(args.OperationId, args.CacheName, args.Key, ct),
        new MutationKeyArgs(operationId, cacheName, key),
        cancellationToken);

    public ValueTask<bool> RemoveExpirationAsync(string operationId, string cacheName, string key, CancellationToken cancellationToken) => WithMappingAsync(
        static (inner, args, ct) => inner.RemoveExpirationAsync(args.OperationId, args.CacheName, args.Key, ct),
        new MutationKeyArgs(operationId, cacheName, key),
        cancellationToken);

    public ValueTask SetEntryAsync(string operationId, string cacheName, string key, NodeCacheEntry<T> entry, CancellationToken cancellationToken) => WithMappingAsync(
        static (inner, args, ct) => inner.SetEntryAsync(args.OperationId, args.CacheName, args.Key, args.Entry, ct),
        new SetEntryArgs(operationId, cacheName, key, entry),
        cancellationToken);

    public ValueTask<bool> TouchAsync(string operationId, string cacheName, string key, TimeSpan expiration, CancellationToken cancellationToken) => WithMappingAsync(
        static (inner, args, ct) => inner.TouchAsync(args.OperationId, args.CacheName, args.Key, args.Expiration, ct),
        new TouchArgs(operationId, cacheName, key, expiration),
        cancellationToken);

    public ValueTask<bool> TryAddEntryAsync(string operationId, string cacheName, string key, NodeCacheEntry<T> entry, CancellationToken cancellationToken) => WithMappingAsync(
        static (inner, args, ct) => inner.TryAddEntryAsync(args.OperationId, args.CacheName, args.Key, args.Entry, ct),
        new SetEntryArgs(operationId, cacheName, key, entry),
        cancellationToken);

    public ValueTask<bool> UpdateAsync(string operationId, string cacheName, string key, T? value, CancellationToken cancellationToken) => WithMappingAsync(
        static (inner, args, ct) => inner.UpdateAsync(args.OperationId, args.CacheName, args.Key, args.Value, ct),
        new UpdateArgs(operationId, cacheName, key, value),
        cancellationToken);

    private async ValueTask WithMappingAsync<TState>(
        Func<ILogicalNamespacedCache<T>, TState, CancellationToken, ValueTask> invoke,
        TState state,
        CancellationToken cancellationToken)
    {
        try
        {
            await invoke(_inner, state, cancellationToken).ConfigureAwait(false);
        }
        catch (RpcException ex)
        {
            DomainTransportErrorMapper.Map(ex, cancellationToken);
        }
    }

    private async ValueTask<TResult> WithMappingAsync<TState, TResult>(
        Func<ILogicalNamespacedCache<T>, TState, CancellationToken, ValueTask<TResult>> invoke,
        TState state,
        CancellationToken cancellationToken)
    {
        try
        {
            return await invoke(_inner, state, cancellationToken).ConfigureAwait(false);
        }
        catch (RpcException ex)
        {
            DomainTransportErrorMapper.Map(ex, cancellationToken);
            return default;
        }
    }

    [Immutable]
    private readonly record struct MutationKeyArgs(string OperationId, string CacheName, string Key);

    [Immutable]
    private readonly record struct ReadKeyArgs(string CacheName, string Key);

    [Immutable]
    private readonly record struct SetEntryArgs(string OperationId, string CacheName, string Key, NodeCacheEntry<T> Entry);

    [Immutable]
    private readonly record struct TouchArgs(string OperationId, string CacheName, string Key, TimeSpan Expiration);

    [Immutable]
    private readonly record struct UpdateArgs(string OperationId, string CacheName, string Key, T? Value);

    /// <summary>
    /// Normalizes selected transport-level <see cref="RpcException" /> failures from the logical cache pipeline
    /// (for example mapping caller cancellation to <see cref="OperationCanceledException" />, counter increment
    /// overflow to <see cref="OverflowException" /> when the server uses the stable overflow contract detail,
    /// increment counter type mismatch to <see cref="InvalidOperationException" /> for the stable type-mismatch detail,
    /// and insert explicit version downgrade to <see cref="InvalidOperationException" /> for the stable insert version precondition detail).
    /// </summary>
    /// <remarks>
    ///     <para>
    ///     This mapper intentionally does not convert <see cref="StatusCode.Unknown" />,
    ///     and does not treat ambiguous outcomes as success.
    ///     </para>
    ///     <para>
    ///     Use <see cref="Map" /> instead of catching and rethrowing <see cref="RpcException" /> manually so original stacks
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
        ///     When a stable domain exception is introduced for a specific <see cref="RpcException" />, throw it with
        ///     <paramref name="ex" /> as <see cref="Exception.InnerException" /> so the original fault context is preserved.
        ///     </para>
        ///     <para>
        ///     When no mapping applies, the original <see cref="RpcException" /> is rethrown with its stack trace preserved.
        ///     </para>
        /// </remarks>
        /// <exception cref="OperationCanceledException">When <paramref name="ex" /> represents caller cancellation.</exception>
        /// <exception cref="OverflowException">When <paramref name="ex" /> represents the stable counter overflow contract.</exception>
        /// <exception cref="InvalidOperationException">
        /// When <paramref name="ex" /> represents the stable increment type-mismatch contract or the stable insert explicit-version precondition
        /// contract.
        /// </exception>
        /// <exception cref="RpcException">When no mapping applies; rethrows <paramref name="ex" /> with preserved stack.</exception>
        [DoesNotReturn]
        internal static void Map(RpcException ex, CancellationToken cancellationToken)
        {
            ArgumentNullException.ThrowIfNull(ex);

            ThrowIfCallerCancellation(ex, cancellationToken);
            ThrowIfInvalidArgumentContract(ex);
            ThrowIfFailedPreconditionContract(ex);
            ThrowIfPayloadTooLargeContract(ex);
            RethrowOriginal(ex);
        }

        [DoesNotReturn]
        private static void RethrowOriginal(RpcException ex) => ExceptionDispatchInfo.Capture(ex).Throw();

        private static void ThrowIfCallerCancellation(RpcException ex, CancellationToken cancellationToken)
        {
            if (ServerCancelClassifier.IsCallerInitiatedGrpcCancellation(ex, cancellationToken))
                cancellationToken.ThrowIfCancellationRequested();
        }

        private static void ThrowIfFailedPreconditionContract(RpcException ex)
        {
            if (ex.StatusCode != StatusCode.FailedPrecondition)
                return;

            if (ServerOpContractClassifier.TryGetFailedPreconditionMessage(ex.Status.Detail, out var message))
                throw new InvalidOperationException(message, ex);

            if (ServerOpContractClassifier.IsOperationIdReuseMismatchDetail(ex.Status.Detail))
                throw new ServerOpIdMismatchException(ex.Status.Detail, ex);
        }

        private static void ThrowIfInvalidArgumentContract(RpcException ex)
        {
            if (ex.StatusCode != StatusCode.InvalidArgument)
                return;

            if (ServerOpContract.IsOperationIdRequiredMessage(ex.Status.Detail))
                throw ServerOpContract.OperationIdRequired();

            if (ServerOpContract.IsOperationIdInvalidFormatMessage(ex.Status.Detail))
                throw ServerOpContract.OperationIdInvalidFormat();

            if (ServerOpContract.IsOperationIdTooLongMessage(ex.Status.Detail))
                throw ServerOpContract.OperationIdTooLong();

            throw new ArgumentException(ex.Status.Detail, nameof(ex), ex);
        }

        private static void ThrowIfPayloadTooLargeContract(RpcException ex)
        {
            if (ex.StatusCode != StatusCode.ResourceExhausted)
                return;

            var detail = ex.Status.Detail;
            if (!detail.StartsWith("Payload size limit is ", StringComparison.Ordinal))
                return;

            throw ServerOpContract.PayloadTooLarge();
        }
    }
}
