using System.Threading;

namespace Squirix.Server.Node.Context;

internal static class RemoteInvocationContext
{
    private static readonly AsyncLocal<bool> InternalOwnerInvocation = new();

    internal static bool IsInternalOwnerInvocation => InternalOwnerInvocation.Value;

    internal static void RestoreInternalOwnerInvocation(bool internalOwnerInvocation) => InternalOwnerInvocation.Value = internalOwnerInvocation;

    internal static RemoteInvocationScope EnterRemoteInvocation(bool isInternalOwnerInvocation = false)
    {
        var value = InternalOwnerInvocation.Value;
        InternalOwnerInvocation.Value = isInternalOwnerInvocation;
        return new RemoteInvocationScope(value);
    }
}
