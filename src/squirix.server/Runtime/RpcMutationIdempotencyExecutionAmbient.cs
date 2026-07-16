using System;
using System.Threading;

namespace Squirix.Server.Runtime;

/// <summary>Ambient marker for RPC idempotency executions that defer journal durability until outcomes are recorded.</summary>
internal static class RpcMutationIdempotencyExecutionAmbient
{
    private static readonly AsyncLocal<object?> ActiveScope = new();

    /// <summary>Gets a value indicating whether durability is currently deferred for an active idempotent RPC.</summary>
    internal static bool IsDeferred => ActiveScope.Value is not null;

    internal static void Activate(object scope) => ActiveScope.Value = scope ?? throw new ArgumentNullException(nameof(scope));

    internal static void Deactivate(object scope)
    {
        if (ReferenceEquals(ActiveScope.Value, scope))
            ActiveScope.Value = null;
    }
}
