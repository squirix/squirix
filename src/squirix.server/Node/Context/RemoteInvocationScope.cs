using System;

namespace Squirix.Server.Node.Context;

/// <summary>Restores <see cref="RemoteInvocationContext" /> async-local state on dispose.</summary>
internal readonly struct RemoteInvocationScope : IDisposable
{
    private readonly bool _internalOwnerInvocation;

    internal RemoteInvocationScope(bool internalOwnerInvocation)
    {
        _internalOwnerInvocation = internalOwnerInvocation;
    }

    public void Dispose() => RemoteInvocationContext.RestoreInternalOwnerInvocation(_internalOwnerInvocation);
}
