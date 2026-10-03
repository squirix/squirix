using System;
using System.Threading;
using System.Threading.Tasks;
using Google.Protobuf;
using Grpc.Core;
using Microsoft.Extensions.Logging;
using Squirix.Server.Attributes;
using Squirix.Server.Errors;
using Squirix.Server.Runtime;
using Squirix.Server.Storage.Journaling.Abstractions;
using Squirix.Server.Utils;

namespace Squirix.Server.Node.Services;

/// <summary>Coordinates replay-or-execute semantics for mutating cache RPC handlers.</summary>
[Immutable]
internal sealed class RpcMutationIdempotencyCoordinator : IRpcMutationIdempotencyCoordinator
{
    private readonly IJournalCoordinator? _journal;
    private readonly ILogger<RpcMutationIdempotencyCoordinator> _logger;
    private readonly RpcMutationIdempotencyStore _store;

    internal RpcMutationIdempotencyCoordinator(RpcMutationIdempotencyStore store, IJournalCoordinator journal, ILogger<RpcMutationIdempotencyCoordinator> logger)
        : this(store, logger)
    {
        ArgumentNullException.ThrowIfNull(journal);
        _journal = journal;
    }

    internal RpcMutationIdempotencyCoordinator(RpcMutationIdempotencyStore store, ILogger<RpcMutationIdempotencyCoordinator> logger)
    {
        ArgumentNullException.ThrowIfNull(store);
        ArgumentNullException.ThrowIfNull(logger);
        _store = store;
        _logger = logger;
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

        // Each pass replays, executes, or joins the execution in flight for this id and then re-reads the record: a joined
        // execution that failed before stamping released its intent, so the next pass may acquire it.
        while (true)
        {
            if (_store.TryReplay(operationId, fingerprint, DefaultParser<TResponse>.Instance, out var cached))
                return cached ?? ReplayGuard.NotCached<TResponse>();

            // Write-ahead intent: record that execution is starting before running the handler so a retry after a
            // crash, or a concurrent duplicate, never re-executes a mutation whose outcome was lost.
            var execution = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            var reservation = _store.ReserveIntent(operationId, fingerprint, execution, out var inFlight);

            if (reservation == IdempotencyReserveResult.Acquired)
                return await ExecuteAcquiredAsync(operationId, fingerprint, state, execute, execution, cancellationToken).ConfigureAwait(false);

            // A concurrent call completed between the replay probe and the reservation: the next pass replays its outcome, or
            // reserves the id anew when that outcome expired in between.
            if (reservation == IdempotencyReserveResult.AlreadyCompleted)
                continue;

            // Started with no execution to join (rebuilt from the journal, or stamped and then failed), or an unknown value: the
            // outcome is unknown to this caller, so surface COMMIT_OUTCOME_UNKNOWN instead of re-executing.
            if (reservation != IdempotencyReserveResult.AlreadyStarted || inFlight == null)
                throw ServerOpContract.CommitOutcomeUnknown().ToRpcException();

            // Execution is in flight in this process: wait for it under this caller's own deadline. The completion never
            // faults; it only signals that the record is settled.
            await inFlight.WaitAsync(cancellationToken).ConfigureAwait(false);
        }
    }

    private async Task<TResponse> ExecuteAcquiredAsync<TState, TResponse>(
        string operationId,
        string fingerprint,
        TState state,
        Func<TState, CancellationToken, Task<TResponse>> execute,
        TaskCompletionSource execution,
        CancellationToken cancellationToken)
        where TResponse : class, IMessage<TResponse>, new()
    {
        try
        {
            return _journal != null ? await ExecuteDurableAsync(_journal, operationId, fingerprint, state, execute, execution, cancellationToken).ConfigureAwait(false)
                : await ExecuteInMemoryAsync(operationId, fingerprint, state, execute, execution, cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            // After the outcome is recorded or the intent released, so joined retries re-read a settled record.
            _store.CompleteExecution(operationId, execution);
        }
    }

    private async Task<TResponse> ExecuteDurableAsync<TState, TResponse>(
        IJournalCoordinator journal,
        string operationId,
        string fingerprint,
        TState state,
        Func<TState, CancellationToken, Task<TResponse>> execute,
        TaskCompletionSource execution,
        CancellationToken cancellationToken)
        where TResponse : class, IMessage<TResponse>, new()
    {
        using var scope = RpcMutationIdempotencyExecutionScope.Begin(operationId, fingerprint, journal, _store, execution);
        try
        {
            var durableResponse = await execute(state, cancellationToken).ConfigureAwait(false);

            // A mutation frame, stamped or applied from a replica group entry, is the decision point: from here only a journal failure
            // or shutdown may stop the outcome from being recorded, never the caller, so a retry replays it instead of seeing
            // COMMIT_OUTCOME_UNKNOWN.
            var tookEffect = RpcMutationIdempotencyExecutionAmbient.HasTakenEffect(scope);
            var responseBytes = await RecordOutcomeDurablyAsync(journal, scope, durableResponse, tookEffect, execution, cancellationToken).ConfigureAwait(false);

            // The in-memory outcome is recorded only after the outcome frame is appended and
            // durability is confirmed: a failure above must leave no Completed record so that
            // a retry surfaces COMMIT_OUTCOME_UNKNOWN instead of replaying an unconfirmed outcome.
            _store.RecordSuccess(operationId, fingerprint, responseBytes, execution);
            return durableResponse;
        }
        catch
        {
            // Release the reservation only when no mutation frame was stamped in this scope. Once a
            // stamped Put/Remove frame is enqueued, the mutation may already be durable: the Started
            // record must survive, so a retry surfaces COMMIT_OUTCOME_UNKNOWN instead of re-executing.
            // The scope is still active here, so the ambient frame reliably reports whether stamping happened.
            // A replicated write is never stamped: its group log is the durable source, so the intent is released and the retry
            // reaches the committer, which replays the group outcome. Reservations restored from the journal or a snapshot are never
            // released here.
            if (!RpcMutationIdempotencyExecutionAmbient.HasStampedMutations(scope))
                _store.ReleaseIntent(operationId, fingerprint, execution);
            throw;
        }
    }

    private async Task<TResponse> ExecuteInMemoryAsync<TState, TResponse>(
        string operationId,
        string fingerprint,
        TState state,
        Func<TState, CancellationToken, Task<TResponse>> execute,
        TaskCompletionSource execution,
        CancellationToken cancellationToken)
        where TResponse : class, IMessage<TResponse>, new()
    {
        try
        {
            var memoryOnlyResponse = await execute(state, cancellationToken).ConfigureAwait(false);
            _store.RecordSuccess(operationId, fingerprint, IdempotencyResponseCodec.SerializeResponseBytes(memoryOnlyResponse), execution);
            return memoryOnlyResponse;
        }
        catch
        {
            // The in-memory path never produces a durable outcome, so releasing the reservation is safe.
            _store.ReleaseIntent(operationId, fingerprint, execution);
            throw;
        }
    }

    /// <summary>Appends the outcome frame and waits for its durability.</summary>
    /// <typeparam name="TResponse">Response type.</typeparam>
    /// <param name="journal">Journal the outcome is appended to.</param>
    /// <param name="scope">Execution scope of the operation.</param>
    /// <param name="response">Response of the executed mutation.</param>
    /// <param name="tookEffect">Whether a mutation frame of this operation was appended (stamped or replicated), so the mutation may have taken effect.</param>
    /// <param name="execution">The completion the reservation was acquired with.</param>
    /// <param name="cancellationToken">Caller cancellation token; honored only before stamping.</param>
    /// <returns>The recorded response bytes.</returns>
    /// <exception cref="RpcException">A stamped mutation whose outcome could not be recorded: COMMIT_OUTCOME_UNKNOWN.</exception>
    private async Task<byte[]> RecordOutcomeDurablyAsync<TResponse>(
        IJournalCoordinator journal,
        RpcMutationIdempotencyExecutionScope scope,
        TResponse response,
        bool tookEffect,
        TaskCompletionSource execution,
        CancellationToken cancellationToken)
        where TResponse : class, IMessage<TResponse>
    {
        var outcomeToken = tookEffect ? CancellationToken.None : cancellationToken;
        try
        {
            var responseBytes = await scope.AppendOutcomeAsync(response, _store, execution, outcomeToken).ConfigureAwait(false);
            await journal.AwaitDurabilityCommitAsync(outcomeToken).ConfigureAwait(false);
            return responseBytes;
        }
        catch (Exception ex) when (tookEffect)
        {
            // The stamped mutation frame may be durable while its outcome is not (shutdown, the failure latch, a rejected outcome
            // frame), so the first caller gets the same unknown outcome a retry gets, never a definite failure.
            ServerLog.DurableMutationOutcomeUnknown(_logger, ex);
            throw ServerOpContract.CommitOutcomeUnknown().ToRpcException();
        }
    }

    private static class DefaultParser<T>
        where T : class, IMessage<T>, new()
    {
        internal static readonly MessageParser<T> Instance = new(static () => new T());
    }

    private static class ReplayGuard
    {
        internal static T NotCached<T>() => throw new InvalidOperationException("Replayed response was not cached.");
    }

    /// <summary>Defers journal durability until idempotency outcome frames are appended for the active RPC.</summary>
    [Immutable]
    private sealed class RpcMutationIdempotencyExecutionScope : IDisposable, IRpcMutationStampListener
    {
        private readonly string _fingerprint;
        private readonly IJournalCoordinator _journal;
        private readonly string _operationId;
        private readonly TaskCompletionSource _reservation;
        private readonly RpcMutationIdempotencyStore _store;

        private RpcMutationIdempotencyExecutionScope(
            string operationId,
            string fingerprint,
            IJournalCoordinator journal,
            RpcMutationIdempotencyStore store,
            TaskCompletionSource reservation)
        {
            _operationId = operationId;
            _fingerprint = fingerprint;
            _journal = journal;
            _store = store;
            _reservation = reservation;
        }

        void IRpcMutationStampListener.OnMutationStamped() => _store.MarkStamped(_operationId, _reservation);

        void IDisposable.Dispose() => RpcMutationIdempotencyExecutionAmbient.Deactivate(this);

        internal static RpcMutationIdempotencyExecutionScope Begin(
            string operationId,
            string fingerprint,
            IJournalCoordinator journal,
            RpcMutationIdempotencyStore store,
            TaskCompletionSource reservation)
        {
            ArgumentException.ThrowIfNullOrWhiteSpace(operationId);
            ArgumentException.ThrowIfNullOrWhiteSpace(fingerprint);
            ArgumentNullException.ThrowIfNull(journal);
            ArgumentNullException.ThrowIfNull(store);
            ArgumentNullException.ThrowIfNull(reservation);

            var scope = new RpcMutationIdempotencyExecutionScope(operationId, fingerprint, journal, store, reservation);
            RpcMutationIdempotencyExecutionAmbient.Activate(scope, operationId, fingerprint);
            return scope;
        }

        internal async ValueTask<byte[]> AppendOutcomeAsync<TResponse>(
            TResponse response,
            RpcMutationIdempotencyStore store,
            TaskCompletionSource reservation,
            CancellationToken cancellationToken)
            where TResponse : class, IMessage<TResponse>
        {
            ArgumentNullException.ThrowIfNull(response);

            var responseBytes = IdempotencyResponseCodec.SerializeResponseBytes(response);
            var (operationId, fingerprint) = (_operationId, _fingerprint);

            // Held under the mutation gate with the frame, so a snapshot cut that covers the frame also exports the outcome.
            await _journal.AppendIdempotencyOutcomeAsync(
                    operationId,
                    fingerprint,
                    responseBytes,
                    () => store.HoldAppendedOutcome(operationId, fingerprint, responseBytes, reservation),
                    cancellationToken)
               .ConfigureAwait(false);
            return responseBytes;
        }
    }
}
