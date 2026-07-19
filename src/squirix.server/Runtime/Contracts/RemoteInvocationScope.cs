using System;

namespace Squirix.Server.Runtime.Contracts;

/// <summary>Restores <see cref="RemoteInvocationContext" /> async-local state on dispose.</summary>
/// <param name="InternalOwnerInvocation">Captured internal-owner flag to restore on dispose.</param>
internal sealed record RemoteInvocationScope(bool InternalOwnerInvocation) : IDisposable
{
    public void Dispose() => RemoteInvocationContext.RestoreInternalOwnerInvocation(InternalOwnerInvocation);
}
