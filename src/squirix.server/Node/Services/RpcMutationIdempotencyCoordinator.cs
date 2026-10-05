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
        using var scope = RpcMutationIdempotencyExecutionScope.Begin(operationId, fingerprint, journal, _store, execution, _logger, typeof(TResponse));
        try
        {
            var durableResponse = await execute(state, cancellationToken).ConfigureAwait(false);

            // The outcome frame went to the journal right after the mutation frame and shared its flush, and the apply recorded the outcome: the
            // response is the one that was appended, so a retry replays exactly what this caller gets.
            if (scope.FusedResponse is { } fused)
            {
                // Never falls through to a second outcome frame: a response of another type than the RPC's is an unknown outcome.
                var fusedResponse = fused as TResponse ?? ThrowHelper.Throw<TResponse>(new InvalidOperationException("The fused outcome is not the response type of the RPC."));
                await scope.ConfirmFusedOutcomeAsync().ConfigureAwait(false);
                return fusedResponse;
            }

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
        catch (Exception ex) when (scope.FusedResponse != null)
        {
            // The outcome frame is on the ring after the mutation frame, so the Started record and its held outcome survive and the first caller
            // gets the same unknown outcome a retry gets; a retry replays the outcome once it is recorded, never re-executes.
            ServerLog.DurableMutationOutcomeUnknown(_logger, ex);
            throw ServerOpContract.CommitOutcomeUnknown().ToRpcException();
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

    /// <summary>Appends the outcome frame and waits for its durability; the mutation frames were already made durable before they were applied.</summary>
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

    /// <summary>Activates the ambient idempotency scope of the active RPC and appends its outcome frame.</summary>
    [Mutable]
    private sealed class RpcMutationIdempotencyExecutionScope : IDisposable, IRpcMutationStampListener, IRpcMutationOutcomeSink
    {
        private readonly string _fingerprint;
        private readonly IJournalCoordinator _journal;
        private readonly ILogger _logger;
        private readonly string _operationId;
        private readonly TaskCompletionSource _reservation;
        private readonly Type _responseType;
        private readonly RpcMutationIdempotencyStore _store;
        private Action? _acceptedCallback;
        private byte[]? _candidateBytes;
        private IMessage? _candidateResponse;
        private byte[]? _fusedBytes;
        private bool _promoted;
        private Delegate? _projection;

        private RpcMutationIdempotencyExecutionScope(
            string operationId,
            string fingerprint,
            IJournalCoordinator journal,
            RpcMutationIdempotencyStore store,
            TaskCompletionSource reservation,
            ILogger logger,
            Type responseType)
        {
            _operationId = operationId;
            _fingerprint = fingerprint;
            _journal = journal;
            _store = store;
            _reservation = reservation;
            _logger = logger;
            _responseType = responseType;
        }

        /// <summary>Gets the response whose outcome frame was appended together with the mutation frame, or <see langword="null" /> when the outcome was not fused.</summary>
        /// <remarks>Set exactly when the outcome frame is known to be on the journal ring, so any failure while it is set is an unknown outcome.</remarks>
        internal IMessage? FusedResponse { get; private set; }

        async ValueTask IRpcMutationOutcomeSink.AppendPredictedOutcomeAsync<TResult>(TResult predicted)
        {
            if (FusedResponse != null || _projection is not Func<TResult, IMessage> project)
                return;

            if (!TryPrepareOutcome(project, predicted))
                return;

            try
            {
                // The token is never canceled: the mutation frame is already on the ring, so this append follows it however the caller goes.
                await _journal.AppendIdempotencyOutcomeAsync(_operationId, _fingerprint, _candidateBytes!, _acceptedCallback ??= OnOutcomeAccepted, CancellationToken.None)
                              .ConfigureAwait(false);
            }
            catch (Exception ex) when (FusedResponse == null && ex is not JournalPostEnqueueFaultException)
            {
                // Refused before the frame reached the ring (admission, shutdown, the failure latch): nothing was appended, so the unfused path
                // appends the outcome after the apply, and a later failure still ends as an unknown outcome through the mutation frame.
                IdempotencyFusionLog.AppendRefused(_logger, ex);
            }
        }

        void IRpcMutationOutcomeSink.PromoteAfterApply()
        {
            if (!_promoted && FusedResponse != null)
                Promote();
        }

        void IRpcMutationOutcomeSink.RegisterProjection<TResult>(Func<TResult, IMessage> projection)
        {
            ArgumentNullException.ThrowIfNull(projection);
            _projection = projection;
        }

        void IRpcMutationStampListener.OnMutationStamped() => _store.MarkStamped(_operationId, _reservation);

        void IDisposable.Dispose() => RpcMutationIdempotencyExecutionAmbient.Deactivate(this);

        internal static RpcMutationIdempotencyExecutionScope Begin(
            string operationId,
            string fingerprint,
            IJournalCoordinator journal,
            RpcMutationIdempotencyStore store,
            TaskCompletionSource reservation,
            ILogger logger,
            Type responseType)
        {
            ArgumentException.ThrowIfNullOrWhiteSpace(operationId);
            ArgumentException.ThrowIfNullOrWhiteSpace(fingerprint);
            ArgumentNullException.ThrowIfNull(journal);
            ArgumentNullException.ThrowIfNull(store);
            ArgumentNullException.ThrowIfNull(reservation);
            ArgumentNullException.ThrowIfNull(logger);
            ArgumentNullException.ThrowIfNull(responseType);

            var scope = new RpcMutationIdempotencyExecutionScope(operationId, fingerprint, journal, store, reservation, logger, responseType);
            RpcMutationIdempotencyExecutionAmbient.Activate(scope, operationId, fingerprint);
            return scope;
        }

        /// <summary>
        /// Makes the fused outcome safe to replay: normally the apply already recorded it, and otherwise (the apply phase did not run) the wait for the
        /// flush that covers the outcome frame is made here before it is recorded.
        /// </summary>
        /// <returns>A task that completes once the outcome is recorded.</returns>
        internal async ValueTask ConfirmFusedOutcomeAsync()
        {
            if (_promoted)
                return;

            await _journal.AwaitDurabilityCommitAsync(CancellationToken.None).ConfigureAwait(false);
            Promote();
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

        private void Promote()
        {
            _promoted = true;
            _store.RecordSuccess(_operationId, _fingerprint, _fusedBytes!, _reservation);
        }

        /// <summary>Called under the journal mutation gate once the outcome frame is on the ring; it sets the fused response before anything else can fail.</summary>
        private void OnOutcomeAccepted()
        {
            _fusedBytes = _candidateBytes;
            FusedResponse = _candidateResponse;
            RpcMutationIdempotencyExecutionAmbient.NotifyOutcomeAppended();
            _store.HoldAppendedOutcome(_operationId, _fingerprint, _candidateBytes!, _reservation);
        }

        /// <summary>Projects the predicted result to the response and serializes it, before anything is appended.</summary>
        /// <typeparam name="TResult">Result type of the durable mutation.</typeparam>
        /// <param name="project">The projection the handler registered.</param>
        /// <param name="predicted">The predicted result.</param>
        /// <returns><see langword="true" /> when the candidate response and its bytes are ready to append.</returns>
        private bool TryPrepareOutcome<TResult>(Func<TResult, IMessage> project, TResult predicted)
        {
            try
            {
                var response = project(predicted);
                if (response.GetType() != _responseType)
                {
                    IdempotencyFusionLog.ProjectionFailed(_logger, new InvalidOperationException("The projected outcome is not the response type of the RPC."));
                    return false;
                }

                _candidateBytes = IdempotencyResponseCodec.SerializeResponseBytes(response);
                _candidateResponse = response;
                return true;
            }
            catch (Exception ex) when (ex is not OutOfMemoryException)
            {
                IdempotencyFusionLog.ProjectionFailed(_logger, ex);
                return false;
            }
        }
    }
}
