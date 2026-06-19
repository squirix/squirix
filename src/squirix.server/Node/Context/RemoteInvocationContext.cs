using System;
using System.Threading;

namespace Squirix.Server.Node.Context;

internal static class RemoteInvocationContext
{
    private static readonly AsyncLocal<bool> InternalOwnerInvocation = new();

    internal static bool IsInternalOwnerInvocation => InternalOwnerInvocation.Value;

    internal static Scope EnterRemoteInvocation(bool isInternalOwnerInvocation = false)
    {
        var value = InternalOwnerInvocation.Value;
        InternalOwnerInvocation.Value = isInternalOwnerInvocation;
        return new Scope(value);
    }

    internal readonly struct Scope : IDisposable
    {
        private readonly bool _internalOwnerInvocation;

        public Scope(bool internalOwnerInvocation)
        {
            _internalOwnerInvocation = internalOwnerInvocation;
        }

        public void Dispose() => InternalOwnerInvocation.Value = _internalOwnerInvocation;
    }
}
