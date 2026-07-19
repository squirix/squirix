using System;
using System.Threading;
using System.Threading.Tasks;
using Google.Protobuf;
using Squirix.Server.Errors;
using Squirix.Server.Runtime;
using Squirix.Server.Storage.Journaling.Abstractions;

namespace Squirix.Server.Node.Services;

/// <summary>Coordinates replay-or-execute semantics for mutating cache RPC handlers.</summary>
internal sealed class RpcMutationIdempotencyCoordinator : IRpcMutationIdempotencyCoordinator
{
    private readonly IJournalCoordinator? _journal;
    private readonly RpcMutationIdempotencyStore _store;

    internal RpcMutationIdempotencyCoordinator(RpcMutationIdempotencyStore store, IJournalCoordinator journal)
        : this(store)
    {
        _journal = journal ?? throw new ArgumentNullException(nameof(journal));
    }

    internal RpcMutationIdempotencyCoordinator(RpcMutationIdempotencyStore store)
    {
        _store = store ?? throw new ArgumentNullException(nameof(store));
    }

    public async Task<TResponse> ExecuteAsync<TState, TResponse>(
        string rawOperationId,
        string fingerprint,
        TState state,
        Func<TState, CancellationToken, Task<TResponse>> execute,
        CancellationToken cancellationToken)
        where TResponse : class, IMessage<TResponse>, new()
    {
        ArgumentNullException.ThrowIfNull(execute);

        var operationId = RpcMutationContracts.RequireOperationId(rawOperationId);
        if (_store.TryReplay(operationId, fingerprint, DefaultParser<TResponse>.Instance, out var cached))
            return cached ?? throw new InvalidOperationException("Replayed response was not cached.");

        if (_journal is not null)
        {
            using var scope = RpcMutationIdempotencyExecutionScope.Begin(
                _store,
                operationId,
                fingerprint,
                _journal,
                static (TResponse typedResponse) => RpcMutationIdempotencyStore.SerializeResponseBytes(typedResponse));
            var durableResponse = await execute(state, cancellationToken).ConfigureAwait(false);
            await scope.CompleteBeforeDurabilityAsync(durableResponse, cancellationToken).ConfigureAwait(false);
            await _journal.AwaitDurabilityCommitAsync(cancellationToken).ConfigureAwait(false);
            return durableResponse;
        }

        var memoryOnlyResponse = await execute(state, cancellationToken).ConfigureAwait(false);
        _store.RecordSuccess(operationId, fingerprint, RpcMutationIdempotencyStore.SerializeResponseBytes(memoryOnlyResponse));
        return memoryOnlyResponse;
    }

    private static class DefaultParser<T>
        where T : IMessage<T>, new()
    {
        internal static readonly MessageParser<T> Instance = new(static () => new T());
    }

    /// <summary>Defers journal durability until idempotency outcome frames are appended for the active RPC.</summary>
    private sealed class RpcMutationIdempotencyExecutionScope : IDisposable
    {
        private readonly Func<object, CancellationToken, ValueTask> _completeAsync;

        private RpcMutationIdempotencyExecutionScope(Func<object, CancellationToken, ValueTask> completeAsync)
        {
            _completeAsync = completeAsync ?? throw new ArgumentNullException(nameof(completeAsync));
        }

        void IDisposable.Dispose() => RpcMutationIdempotencyExecutionAmbient.Deactivate(this);

        internal static RpcMutationIdempotencyExecutionScope Begin<TResponse>(
            RpcMutationIdempotencyStore store,
            string operationId,
            string fingerprint,
            IJournalCoordinator journal,
            Func<TResponse, byte[]> serializeResponse)
            where TResponse : IMessage<TResponse>
        {
            ArgumentNullException.ThrowIfNull(store);
            ArgumentException.ThrowIfNullOrWhiteSpace(operationId);
            ArgumentException.ThrowIfNullOrWhiteSpace(fingerprint);
            ArgumentNullException.ThrowIfNull(journal);
            ArgumentNullException.ThrowIfNull(serializeResponse);

            var scope = new RpcMutationIdempotencyExecutionScope(CompleteAsync);
            RpcMutationIdempotencyExecutionAmbient.Activate(scope);
            return scope;

            ValueTask CompleteAsync(object response, CancellationToken cancellationToken)
            {
                if (response is not TResponse typedResponse)
                    throw new InvalidOperationException("Idempotency response type does not match the active execution scope.");

                var responseBytes = serializeResponse(typedResponse);
                store.RecordSuccess(operationId, fingerprint, responseBytes);
                return journal.AppendIdempotencyOutcomeAsync(operationId, fingerprint, responseBytes, cancellationToken);
            }
        }

        internal ValueTask CompleteBeforeDurabilityAsync<TResponse>(TResponse response, CancellationToken cancellationToken)
            where TResponse : IMessage<TResponse>
        {
            ArgumentNullException.ThrowIfNull(response);
            return _completeAsync(response, cancellationToken);
        }
    }
}
