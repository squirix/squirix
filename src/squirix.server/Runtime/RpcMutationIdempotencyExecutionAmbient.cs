using System;
using System.Threading;
using System.Threading.Tasks;
using Google.Protobuf;

namespace Squirix.Server.Runtime;

/// <summary>Ambient marker for RPC idempotency executions: it stamps mutation frames with the operation id and tracks whether the scope took effect.</summary>
internal static class RpcMutationIdempotencyExecutionAmbient
{
    private static readonly AsyncLocal<ScopeFrame?> Current = new();

    /// <summary>Scope of the frame that marks a replicated apply running outside every RPC scope; it equals no scope a caller activates.</summary>
    private static readonly object ScopelessSentinel = new();

    /// <summary>Gets the operation identifier journal mutation frames are stamped with, or <see langword="null" /> when no scope is active or stamping is suspended.</summary>
    internal static string? ActiveOperationIdValue => Current.Value is { StampingSuspended: false } frame ? frame.OperationId : null;

    /// <summary>Gets the request fingerprint journal mutation frames are stamped with, or <see langword="null" /> when no scope is active or stamping is suspended.</summary>
    internal static string? ActiveFingerprintValue => Current.Value is { StampingSuspended: false } frame ? frame.Fingerprint : null;

    /// <summary>Gets a value indicating whether the running write is a replicated apply, whose durable source is the replica group log and not the cache journal; true with or without an active scope.</summary>
    internal static bool IsStampingSuspended => Current.Value is { StampingSuspended: true };

    /// <summary>Gets a value indicating whether the active scope already appended its outcome frame, so no cache mutation frame may follow it.</summary>
    internal static bool HasAppendedOutcome => Current.Value?.State.OutcomeAppended ?? false;

    /// <summary>
    /// Gets the sink an outcome frame is appended through: the active scope when it stamped its mutation frame, has not appended an outcome and runs a
    /// write whose durable source is the cache journal; otherwise <see langword="null" />.
    /// </summary>
    private static IRpcMutationOutcomeSink? AppendableOutcomeSink =>
        Current.Value is { StampingSuspended: false, State: { Stamped: true, OutcomeAppended: false } } frame ? frame.Scope as IRpcMutationOutcomeSink : null;

    /// <summary>Gets the sink of the active scope when its stamping is not suspended; otherwise <see langword="null" />.</summary>
    private static IRpcMutationOutcomeSink? ActiveOutcomeSink => Current.Value is { StampingSuspended: false } frame ? frame.Scope as IRpcMutationOutcomeSink : null;

    internal static void Activate(object scope, string operationId, string fingerprint)
    {
        ArgumentNullException.ThrowIfNull(scope);
        ArgumentException.ThrowIfNullOrWhiteSpace(operationId);
        ArgumentException.ThrowIfNullOrWhiteSpace(fingerprint);
        Current.Value = new ScopeFrame(scope, operationId, fingerprint, Current.Value, false, new ScopeState());
    }

    /// <summary>Stops stamping mutation frames with the active operation id until the returned value is disposed.</summary>
    /// <returns>The value that restores the previous stamping state when disposed.</returns>
    /// <remarks>
    /// Mutations inside an active scope still count as having taken effect; the write-ahead stamp is dropped and the executor skips the wait
    /// for the cache journal flush before the apply, for writes whose durable source is not the cache journal. Without an active scope the
    /// suspension pushes a frame of its own that belongs to no scope, so the same applies run unstamped and skip the wait there too; disposing
    /// it always restores the previous frame, which keeps the push and the restore balanced on every path.
    /// </remarks>
    internal static SuspendedStamping SuspendStamping()
    {
        var previous = Current.Value;
        Current.Value = previous != null
            ? new ScopeFrame(previous.Scope, previous.OperationId, previous.Fingerprint, previous, true, previous.State)
            : new ScopeFrame(ScopelessSentinel, string.Empty, string.Empty, null, true, new ScopeState());

        return new SuspendedStamping(previous);
    }

    internal static void Deactivate(object scope)
    {
        if (!ReferenceEquals(Current.Value?.Scope, scope))
            return;
        Current.Value = Current.Value.Parent;
    }

    /// <summary>Determines whether the given scope stamped any mutation while it was active.</summary>
    /// <param name="scope">The scope to inspect.</param>
    /// <returns><see langword="true" /> when the scope stamped at least one mutation frame.</returns>
    internal static bool HasStampedMutations(object scope) => FindState(scope)?.Stamped ?? false;

    /// <summary>Determines whether a mutation appended under the given scope, stamped or not, may have taken effect.</summary>
    /// <param name="scope">The scope to inspect.</param>
    /// <returns><see langword="true" /> when the scope appended at least one cache mutation frame.</returns>
    internal static bool HasTakenEffect(object scope) => FindState(scope)?.TakenEffect ?? false;

    /// <summary>Marks the active scope as having appended its outcome frame, after which it refuses every further cache mutation frame.</summary>
    internal static void NotifyOutcomeAppended()
    {
        if (Current.Value is { } frame)
            frame.State.OutcomeAppended = true;
    }

    /// <summary>
    /// Appends the outcome of the running operation from the result its mutation will have, when the active scope can take it: it stamped its mutation
    /// frame, has not appended an outcome yet and runs a write whose durable source is the cache journal; otherwise nothing is appended.
    /// </summary>
    /// <typeparam name="TResult">Result type of the durable mutation.</typeparam>
    /// <param name="predicted">The result the mutation will have once applied.</param>
    /// <returns>A task that completes once the outcome frame is on the journal ring, or was not appended.</returns>
    internal static ValueTask AppendPredictedOutcomeAsync<TResult>(TResult predicted) =>
        AppendableOutcomeSink is { } sink ? sink.AppendPredictedOutcomeAsync(predicted) : ValueTask.CompletedTask;

    /// <summary>Records the outcome the active scope appended as completed, right after its mutation was applied; does nothing without such a scope.</summary>
    internal static void PromoteOutcomeAfterApply() => ActiveOutcomeSink?.PromoteAfterApply();

    /// <summary>Registers, in the active scope, the projection of a mutation result to the response of the running RPC; does nothing without a scope.</summary>
    /// <typeparam name="TResult">Result type of the durable mutation.</typeparam>
    /// <param name="projection">Builds the response from the predicted result.</param>
    internal static void RegisterOutcomeProjection<TResult>(Func<TResult, IMessage> projection) => ActiveOutcomeSink?.RegisterProjection(projection);

    /// <summary>Marks the active scope as having appended a mutation frame that was not stamped.</summary>
    internal static void NotifyMutationApplied()
    {
        if (Current.Value is { } frame)
            frame.State.TakenEffect = true;
    }

    /// <summary>Marks the active scope as having stamped at least one durable mutation frame.</summary>
    internal static void NotifyMutationStamped()
    {
        if (Current.Value is not { } frame)
            return;

        frame.State.TakenEffect = true;
        frame.State.Stamped = true;
        (frame.Scope as IRpcMutationStampListener)?.OnMutationStamped();
    }

    private static ScopeState? FindState(object scope)
    {
        for (var frame = Current.Value; frame != null; frame = frame.Parent)
        {
            if (ReferenceEquals(frame.Scope, scope))
                return frame.State;
        }

        return null;
    }

    /// <summary>Restores the stamping state that was active before <see cref="SuspendStamping" />.</summary>
    /// <param name="Previous">The frame active before the suspension; <see langword="null" /> when no scope was active, which is then the state restored.</param>
    internal readonly record struct SuspendedStamping(ScopeFrame? Previous) : IDisposable
    {
        /// <inheritdoc />
        public void Dispose() => Current.Value = Previous;
    }

    /// <summary>Scope state shared by the frame of a scope and the frames that suspend its stamping.</summary>
    internal sealed class ScopeState
    {
        internal bool OutcomeAppended { get; set; }

        internal bool Stamped { get; set; }

        internal bool TakenEffect { get; set; }
    }

    internal sealed class ScopeFrame
    {
        internal ScopeFrame(object scope, string operationId, string fingerprint, ScopeFrame? parent, bool stampingSuspended, ScopeState state)
        {
            Scope = scope;
            OperationId = operationId;
            Fingerprint = fingerprint;
            Parent = parent;
            StampingSuspended = stampingSuspended;
            State = state;
        }

        internal string Fingerprint { get; }

        internal string OperationId { get; }

        internal ScopeFrame? Parent { get; }

        internal object Scope { get; }

        internal ScopeState State { get; }

        internal bool StampingSuspended { get; }
    }
}
