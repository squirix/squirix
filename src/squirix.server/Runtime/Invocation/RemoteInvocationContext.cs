using System.Threading;

namespace Squirix.Server.Runtime.Invocation;

/// <summary>Async-local remote-invocation classification used by server endpoint handlers.</summary>
internal static class RemoteInvocationContext
{
    /// <summary>A shared marker represents true without boxing a Boolean on every context change; null represents false.</summary>
    private static readonly object InternalOwnerMarker = new();
    private static readonly AsyncLocal<object?> Invocation = new();

    internal static bool IsInternalOwnerInvocation => Invocation.Value != null;

    internal static RemoteInvocationScope EnterRemoteInvocation(bool isInternalOwnerInvocation = false)
    {
        var previous = IsInternalOwnerInvocation;
        if (previous != isInternalOwnerInvocation)
            Invocation.Value = isInternalOwnerInvocation ? InternalOwnerMarker : null;
        return new RemoteInvocationScope(previous);
    }

    internal static void RestoreInvocation(bool previous)
    {
        if (IsInternalOwnerInvocation != previous)
            Invocation.Value = previous ? InternalOwnerMarker : null;
    }
}
