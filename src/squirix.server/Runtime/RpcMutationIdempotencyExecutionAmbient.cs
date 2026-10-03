using System;
using System.Threading;

namespace Squirix.Server.Runtime;

/// <summary>Ambient marker for RPC idempotency executions that defer journal durability until outcomes are recorded.</summary>
internal static class RpcMutationIdempotencyExecutionAmbient
{
    private static readonly AsyncLocal<ScopeFrame?> Current = new();

    /// <summary>Gets the operation identifier journal mutation frames are stamped with, or <see langword="null" /> when no scope is active or stamping is suspended.</summary>
    internal static string? ActiveOperationIdValue => Current.Value is { StampingSuspended: false } frame ? frame.OperationId : null;

    /// <summary>Gets the request fingerprint journal mutation frames are stamped with, or <see langword="null" /> when no scope is active or stamping is suspended.</summary>
    internal static string? ActiveFingerprintValue => Current.Value is { StampingSuspended: false } frame ? frame.Fingerprint : null;

    /// <summary>Gets a value indicating whether durability is currently deferred for an active idempotent RPC.</summary>
    internal static bool IsDeferred => Current.Value != null;

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
    /// Durability stays deferred and mutations still count as having taken effect; only the write-ahead stamp is dropped, for writes whose
    /// durable source is not the cache journal.
    /// </remarks>
    internal static SuspendedStamping SuspendStamping()
    {
        var previous = Current.Value;
        if (previous != null)
            Current.Value = new ScopeFrame(previous.Scope, previous.OperationId, previous.Fingerprint, previous, true, previous.State);

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
    internal readonly struct SuspendedStamping : IDisposable
    {
        private readonly ScopeFrame? _previous;

        internal SuspendedStamping(ScopeFrame? previous)
        {
            _previous = previous;
        }

        /// <inheritdoc />
        public void Dispose()
        {
            if (_previous != null)
                Current.Value = _previous;
        }
    }

    /// <summary>Scope state shared by the frame of a scope and the frames that suspend its stamping.</summary>
    internal sealed class ScopeState
    {
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
