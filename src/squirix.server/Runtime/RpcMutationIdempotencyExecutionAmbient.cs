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
        Current.Value = new ScopeFrame(scope, operationId, fingerprint, Current.Value, false);
    }

    /// <summary>Stops stamping mutation frames with the active operation id until <see cref="ResumeStamping" /> is called with the returned token.</summary>
    /// <returns>The token to pass to <see cref="ResumeStamping" />.</returns>
    /// <remarks>Durability stays deferred; only the write-ahead stamp is dropped, for writes whose durable source is not the cache journal.</remarks>
    internal static object? SuspendStamping()
    {
        var previous = Current.Value;
        if (previous == null)
            return null;

        Current.Value = new ScopeFrame(previous.Scope, previous.OperationId, previous.Fingerprint, previous, true);
        return previous;
    }

    /// <summary>Restores the stamping state captured by <see cref="SuspendStamping" />.</summary>
    /// <param name="token">The token returned by <see cref="SuspendStamping" />.</param>
    internal static void ResumeStamping(object? token)
    {
        if (token is ScopeFrame previous)
            Current.Value = previous;
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
    internal static bool HasStampedMutations(object scope)
    {
        for (var frame = Current.Value; frame != null; frame = frame.Parent)
        {
            if (ReferenceEquals(frame.Scope, scope) && frame.MutationStamped)
                return true;
        }

        return false;
    }

    /// <summary>Marks the active scope as having stamped at least one durable mutation frame.</summary>
    internal static void NotifyMutationStamped() => Current.Value?.MarkStamped();

    private sealed class ScopeFrame
    {
        internal ScopeFrame(object scope, string operationId, string fingerprint, ScopeFrame? parent, bool stampingSuspended)
        {
            Scope = scope;
            OperationId = operationId;
            Fingerprint = fingerprint;
            Parent = parent;
            StampingSuspended = stampingSuspended;
        }

        internal string Fingerprint { get; }

        internal bool MutationStamped { get; private set; }

        internal string OperationId { get; }

        internal ScopeFrame? Parent { get; }

        internal object Scope { get; }

        internal bool StampingSuspended { get; }

        internal void MarkStamped() => MutationStamped = true;
    }
}
