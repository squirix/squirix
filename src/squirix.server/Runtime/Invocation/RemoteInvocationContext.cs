using System.Threading;

namespace Squirix.Server.Runtime.Invocation;

/// <summary>Async-local remote-invocation classification used by server endpoint handlers.</summary>
internal static class RemoteInvocationContext
{
    /// <summary>A shared marker represents true without boxing a Boolean on every context change; null represents false.</summary>
    private static readonly object InternalOwnerMarker = new();
    private static readonly object PublicMarker = new();

    private static readonly AsyncLocal<object?> Invocation = new();

    internal static bool IsInternalOwnerInvocation => ReferenceEquals(Invocation.Value, InternalOwnerMarker);

    internal static bool IsRemoteInvocation => Invocation.Value != null;

    internal static RemoteInvocationScope EnterRemoteInvocation(bool isInternalOwnerInvocation = false)
    {
        var previous = Invocation.Value;
        Invocation.Value = isInternalOwnerInvocation ? InternalOwnerMarker : PublicMarker;
        return new RemoteInvocationScope(previous);
    }

    internal static void RestoreInvocation(object? previous) => Invocation.Value = previous;
}
