using System;
using Squirix.Attributes;

namespace Squirix.Server.Runtime.Invocation;

/// <summary>Restores <see cref="RemoteInvocationContext" /> async-local state on dispose.</summary>
/// <param name="InternalOwnerInvocation">Captured internal-owner flag to restore on dispose.</param>
[Immutable]
internal sealed record RemoteInvocationScope(bool InternalOwnerInvocation) : IDisposable
{
    public void Dispose() => RemoteInvocationContext.RestoreInternalOwnerInvocation(InternalOwnerInvocation);
}
