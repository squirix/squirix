using System;
using System.Threading;

namespace Squirix.Server.Node.Context;

internal static class RemoteInvocationContext
{
    private static readonly AsyncLocal<bool> InternalOwnerInvocation = new();
    private static readonly AsyncLocal<bool> RemoteInvocation = new();

    public static bool IsInternalOwnerInvocation => InternalOwnerInvocation.Value;

    internal static Scope EnterRemoteInvocation(bool isInternalOwnerInvocation = false)
    {
        var wasRemoteInvocation = RemoteInvocation.Value;
        var wasInternalOwnerInvocation = InternalOwnerInvocation.Value;
        RemoteInvocation.Value = true;
        InternalOwnerInvocation.Value = isInternalOwnerInvocation;
        return new Scope(wasRemoteInvocation, wasInternalOwnerInvocation);
    }

    internal readonly struct Scope : IDisposable
    {
        private readonly bool _wasInternalOwnerInvocation;
        private readonly bool _wasRemoteInvocation;

        public Scope(bool wasRemoteInvocation, bool wasInternalOwnerInvocation)
        {
            _wasInternalOwnerInvocation = wasInternalOwnerInvocation;
            _wasRemoteInvocation = wasRemoteInvocation;
        }

        public void Dispose()
        {
            InternalOwnerInvocation.Value = _wasInternalOwnerInvocation;
            RemoteInvocation.Value = _wasRemoteInvocation;
        }
    }
}
