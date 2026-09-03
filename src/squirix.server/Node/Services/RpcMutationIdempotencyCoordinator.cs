using System;
using System.Threading;
using System.Threading.Tasks;
using Google.Protobuf;
using Squirix.Server.Attributes;
using Squirix.Server.Errors;
using Squirix.Server.Runtime;
using Squirix.Server.Storage.Journaling.Abstractions;

namespace Squirix.Server.Node.Services;

/// <summary>Coordinates replay-or-execute semantics for mutating cache RPC handlers.</summary>
[Immutable]
internal sealed class RpcMutationIdempotencyCoordinator : IRpcMutationIdempotencyCoordinator
{
    private readonly IJournalCoordinator? _journal;
    private readonly RpcMutationIdempotencyStore _store;

    internal RpcMutationIdempotencyCoordinator(RpcMutationIdempotencyStore store, IJournalCoordinator journal)
        : this(store)
    {
        ArgumentNullException.ThrowIfNull(journal);
        _journal = journal;
    }

    internal RpcMutationIdempotencyCoordinator(RpcMutationIdempotencyStore store)
    {
        ArgumentNullException.ThrowIfNull(store);
        _store = store;
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

        if (_journal != null)
            await _journal.WaitForStartupAsync(cancellationToken).ConfigureAwait(false);

        if (_store.TryReplay(operationId, fingerprint, DefaultParser<TResponse>.Instance, out var cached))
        {
            if (cached == null)
                throw new InvalidOperationException("Replayed response was not cached.");

            return cached;
        }

        if (_journal != null)
        {
            using var scope = RpcMutationIdempotencyExecutionScope.Begin(_store, operationId, fingerprint, _journal);
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
        where T : class, IMessage<T>, new()
    {
        internal static readonly MessageParser<T> Instance = new(static () => new T());
    }

    /// <summary>Defers journal durability until idempotency outcome frames are appended for the active RPC.</summary>
    [Immutable]
    private sealed class RpcMutationIdempotencyExecutionScope : IDisposable
    {
        private readonly string _fingerprint;
        private readonly IJournalCoordinator _journal;
        private readonly string _operationId;
        private readonly RpcMutationIdempotencyStore _store;

        private RpcMutationIdempotencyExecutionScope(RpcMutationIdempotencyStore store, string operationId, string fingerprint, IJournalCoordinator journal)
        {
            _store = store;
            _operationId = operationId;
            _fingerprint = fingerprint;
            _journal = journal;
        }

        void IDisposable.Dispose() => RpcMutationIdempotencyExecutionAmbient.Deactivate(this);

        internal static RpcMutationIdempotencyExecutionScope Begin(RpcMutationIdempotencyStore store, string operationId, string fingerprint, IJournalCoordinator journal)
        {
            ArgumentNullException.ThrowIfNull(store);
            ArgumentException.ThrowIfNullOrWhiteSpace(operationId);
            ArgumentException.ThrowIfNullOrWhiteSpace(fingerprint);
            ArgumentNullException.ThrowIfNull(journal);

            var scope = new RpcMutationIdempotencyExecutionScope(store, operationId, fingerprint, journal);
            RpcMutationIdempotencyExecutionAmbient.Activate(scope);
            return scope;
        }

        internal ValueTask CompleteBeforeDurabilityAsync<TResponse>(TResponse response, CancellationToken cancellationToken)
            where TResponse : class, IMessage<TResponse>
        {
            ArgumentNullException.ThrowIfNull(response);

            var responseBytes = RpcMutationIdempotencyStore.SerializeResponseBytes(response);
            _store.RecordSuccess(_operationId, _fingerprint, responseBytes);
            return _journal.AppendIdempotencyOutcomeAsync(_operationId, _fingerprint, responseBytes, cancellationToken);
        }
    }
}
