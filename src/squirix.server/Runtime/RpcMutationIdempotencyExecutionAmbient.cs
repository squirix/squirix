using System;
using System.Threading;

namespace Squirix.Server.Runtime;

/// <summary>Ambient marker for RPC idempotency executions that defer journal durability until outcomes are recorded.</summary>
internal static class RpcMutationIdempotencyExecutionAmbient
{
    private static readonly AsyncLocal<ScopeFrame?> Current = new();

    /// <summary>
    /// Gets the operation identifier of the active idempotent RPC, or <see langword="null" /> when no scope is active.
    /// </summary>
    internal static string? ActiveOperationIdValue => Current.Value?.OperationId;

    /// <summary>Gets a value indicating whether durability is currently deferred for an active idempotent RPC.</summary>
    internal static bool IsDeferred => Current.Value != null;

    internal static void Activate(object scope, string operationId)
    {
        ArgumentNullException.ThrowIfNull(scope);
        ArgumentException.ThrowIfNullOrWhiteSpace(operationId);
        Current.Value = new ScopeFrame(scope, operationId, Current.Value);
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
            if (ReferenceEquals(frame.Scope, scope))
                return frame.MutationStamped;
        }

        return false;
    }

    /// <summary>Marks the active scope as having stamped at least one durable mutation frame.</summary>
    internal static void NotifyMutationStamped() => Current.Value?.MarkStamped();

    private sealed class ScopeFrame
    {
        internal ScopeFrame(object scope, string operationId, ScopeFrame? parent)
        {
            Scope = scope;
            OperationId = operationId;
            Parent = parent;
        }

        internal bool MutationStamped { get; private set; }

        internal string OperationId { get; }

        internal ScopeFrame? Parent { get; }

        internal object Scope { get; }

        internal void MarkStamped() => MutationStamped = true;
    }
}
