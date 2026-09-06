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

        // Write-ahead intent: record that execution is starting before running the handler so a retry after a
        // crash, or a concurrent duplicate, never re-executes a mutation whose outcome was lost.
        var reservation = _store.ReserveIntent(operationId, fingerprint);
        if (reservation == IdempotencyReserveResult.AlreadyCompleted)
        {
            // A concurrent call completed between the replay probe and the reservation; replay its outcome.
            if (_store.TryReplay(operationId, fingerprint, DefaultParser<TResponse>.Instance, out var completed))
                return completed!;

            throw new InvalidOperationException("Idempotency reservation completed without a replayed outcome.");
        }

        if (reservation != IdempotencyReserveResult.Acquired)
            throw ServerOpContract.CommitOutcomeUnknown().ToRpcException();

        return await ExecuteAcquiredAsync(operationId, fingerprint, state, execute, cancellationToken).ConfigureAwait(false);
    }

    private async Task<TResponse> ExecuteAcquiredAsync<TState, TResponse>(
        string operationId,
        string fingerprint,
        TState state,
        Func<TState, CancellationToken, Task<TResponse>> execute,
        CancellationToken cancellationToken)
        where TResponse : class, IMessage<TResponse>, new()
    {
        if (_journal != null)
        {
            using var scope = RpcMutationIdempotencyExecutionScope.Begin(operationId, fingerprint, _journal);
            try
            {
                var durableResponse = await execute(state, cancellationToken).ConfigureAwait(false);
                var responseBytes = await scope.AppendOutcomeAsync(durableResponse, cancellationToken).ConfigureAwait(false);
                await _journal.AwaitDurabilityCommitAsync(cancellationToken).ConfigureAwait(false);

                // The in-memory outcome is recorded only after the outcome frame is appended and
                // durability is confirmed: a failure above must leave no Completed record so that
                // a retry surfaces COMMIT_OUTCOME_UNKNOWN instead of replaying an unconfirmed outcome.
                _store.RecordSuccess(operationId, fingerprint, responseBytes);
                return durableResponse;
            }
            catch
            {
                // Release the reservation only when no mutation frame was stamped in this scope. Once a
                // stamped Put/Remove frame is enqueued, the mutation may already be durable: the Started
                // record must survive, so a retry surfaces COMMIT_OUTCOME_UNKNOWN instead of re-executing.
                // The scope is still active here, so the ambient frame reliably reports whether stamping happened.
                // Reserved intents reconstructed from journal frames carry no fingerprint and are never released here.
                if (!RpcMutationIdempotencyExecutionAmbient.HasStampedMutations(scope))
                    _store.ReleaseIntent(operationId, fingerprint);
                throw;
            }
        }

        try
        {
            var memoryOnlyResponse = await execute(state, cancellationToken).ConfigureAwait(false);
            _store.RecordSuccess(operationId, fingerprint, IdempotencyResponseCodec.SerializeResponseBytes(memoryOnlyResponse));
            return memoryOnlyResponse;
        }
        catch
        {
            // The in-memory path never produces a durable outcome, so releasing the reservation is safe.
            _store.ReleaseIntent(operationId, fingerprint);
            throw;
        }
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

        private RpcMutationIdempotencyExecutionScope(string operationId, string fingerprint, IJournalCoordinator journal)
        {
            _operationId = operationId;
            _fingerprint = fingerprint;
            _journal = journal;
        }

        void IDisposable.Dispose() => RpcMutationIdempotencyExecutionAmbient.Deactivate(this);

        internal static RpcMutationIdempotencyExecutionScope Begin(string operationId, string fingerprint, IJournalCoordinator journal)
        {
            ArgumentException.ThrowIfNullOrWhiteSpace(operationId);
            ArgumentException.ThrowIfNullOrWhiteSpace(fingerprint);
            ArgumentNullException.ThrowIfNull(journal);

            var scope = new RpcMutationIdempotencyExecutionScope(operationId, fingerprint, journal);
            RpcMutationIdempotencyExecutionAmbient.Activate(scope, operationId);
            return scope;
        }

        internal async ValueTask<byte[]> AppendOutcomeAsync<TResponse>(TResponse response, CancellationToken cancellationToken)
            where TResponse : class, IMessage<TResponse>
        {
            ArgumentNullException.ThrowIfNull(response);

            var responseBytes = IdempotencyResponseCodec.SerializeResponseBytes(response);
            await _journal.AppendIdempotencyOutcomeAsync(_operationId, _fingerprint, responseBytes, cancellationToken).ConfigureAwait(false);
            return responseBytes;
        }
    }
}
