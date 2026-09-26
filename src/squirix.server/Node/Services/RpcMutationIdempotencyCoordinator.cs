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

    /// <summary>Gets the logger for commit-unknown causes; the host logger unless set.</summary>
    internal ILogger Log { private get; init; } = LogManager.GetLogger<RpcMutationIdempotencyCoordinator>();

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

            // Execution is in flight in this process: wait for it under this caller's own deadline. The completion never
            // faults; it only signals that the record is settled.
            if (reservation != IdempotencyReserveResult.AlreadyStarted || inFlight == null)
            {
                return reservation switch
                {
                    // A concurrent call completed between the replay probe and the reservation; replay its outcome.
                    IdempotencyReserveResult.AlreadyCompleted => _store.TryReplay(operationId, fingerprint, DefaultParser<TResponse>.Instance, out var completed) ? completed!
                        : throw new InvalidOperationException("Idempotency reservation completed without a replayed outcome."),
                    IdempotencyReserveResult.Acquired => await ExecuteAcquiredAsync(operationId, fingerprint, state, execute, execution, cancellationToken).ConfigureAwait(false),

                    // Started with no execution to join (rebuilt from the journal, or stamped and then failed), or an unknown
                    // value: the outcome is unknown to this caller, so surface COMMIT_OUTCOME_UNKNOWN instead of re-executing.
                    IdempotencyReserveResult.AlreadyStarted => throw ServerOpContract.CommitOutcomeUnknown().ToRpcException(),
                    _ => throw ServerOpContract.CommitOutcomeUnknown().ToRpcException(),
                };
            }

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
            return _journal != null ? await ExecuteDurableAsync(_journal, operationId, fingerprint, state, execute, cancellationToken).ConfigureAwait(false)
                : await ExecuteInMemoryAsync(operationId, fingerprint, state, execute, cancellationToken).ConfigureAwait(false);
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
        CancellationToken cancellationToken)
        where TResponse : class, IMessage<TResponse>, new()
    {
        using var scope = RpcMutationIdempotencyExecutionScope.Begin(operationId, fingerprint, journal);
        try
        {
            var durableResponse = await execute(state, cancellationToken).ConfigureAwait(false);

            // A stamped mutation frame is the decision point: from here only a journal failure or shutdown may stop the
            // outcome from being recorded, never the caller, so a retry replays it instead of seeing COMMIT_OUTCOME_UNKNOWN.
            var stamped = RpcMutationIdempotencyExecutionAmbient.HasStampedMutations(scope);
            var responseBytes = await RecordOutcomeDurablyAsync(journal, scope, durableResponse, stamped, cancellationToken).ConfigureAwait(false);

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

    private async Task<TResponse> ExecuteInMemoryAsync<TState, TResponse>(
        string operationId,
        string fingerprint,
        TState state,
        Func<TState, CancellationToken, Task<TResponse>> execute,
        CancellationToken cancellationToken)
        where TResponse : class, IMessage<TResponse>, new()
    {
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

    /// <summary>Appends the outcome frame and waits for its durability.</summary>
    /// <typeparam name="TResponse">Response type.</typeparam>
    /// <param name="journal">Journal the outcome is appended to.</param>
    /// <param name="scope">Execution scope of the operation.</param>
    /// <param name="response">Response of the executed mutation.</param>
    /// <param name="stamped">Whether a mutation frame of this operation was stamped (and may be durable).</param>
    /// <param name="cancellationToken">Caller cancellation token; honored only before stamping.</param>
    /// <returns>The recorded response bytes.</returns>
    /// <exception cref="RpcException">A stamped mutation whose outcome could not be recorded: COMMIT_OUTCOME_UNKNOWN.</exception>
    private async Task<byte[]> RecordOutcomeDurablyAsync<TResponse>(
        IJournalCoordinator journal,
        RpcMutationIdempotencyExecutionScope scope,
        TResponse response,
        bool stamped,
        CancellationToken cancellationToken)
        where TResponse : class, IMessage<TResponse>
    {
        var outcomeToken = stamped ? CancellationToken.None : cancellationToken;
        try
        {
            var responseBytes = await scope.AppendOutcomeAsync(response, outcomeToken).ConfigureAwait(false);
            await journal.AwaitDurabilityCommitAsync(outcomeToken).ConfigureAwait(false);
            return responseBytes;
        }
        catch (Exception ex) when (stamped)
        {
            // The stamped mutation frame may be durable while its outcome is not (shutdown, the failure latch, a rejected outcome
            // frame), so the first caller gets the same unknown outcome a retry gets, never a definite failure.
            LogManager.DurableMutationOutcomeUnknown(Log, ex);
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
