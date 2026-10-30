using System.Threading;

namespace Squirix.Server.Runtime.Invocation;

/// <summary>Async-local remote-invocation classification used by server endpoint handlers.</summary>
internal static class RemoteInvocationContext
{
    /// <summary>A shared marker represents true without boxing a Boolean on every context change; null represents false.</summary>
    private static readonly object InternalOwnerMarker = new();

    private static readonly AsyncLocal<object?> InternalOwnerInvocation = new();

    internal static bool IsInternalOwnerInvocation => InternalOwnerInvocation.Value != null;

    internal static RemoteInvocationScope EnterRemoteInvocation(bool isInternalOwnerInvocation = false)
    {
        var value = IsInternalOwnerInvocation;
        if (value != isInternalOwnerInvocation)
            InternalOwnerInvocation.Value = isInternalOwnerInvocation ? InternalOwnerMarker : null;
        return new RemoteInvocationScope(value);
    }

    internal static void RestoreInternalOwnerInvocation(bool internalOwnerInvocation)
    {
        if (IsInternalOwnerInvocation != internalOwnerInvocation)
            InternalOwnerInvocation.Value = internalOwnerInvocation ? InternalOwnerMarker : null;
    }
}
