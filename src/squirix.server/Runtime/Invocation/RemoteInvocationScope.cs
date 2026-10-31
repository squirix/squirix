using System;
using System.Runtime.InteropServices;
using Squirix.Server.Attributes;

namespace Squirix.Server.Runtime.Invocation;

/// <summary>Restores <see cref="RemoteInvocationContext" /> async-local state on dispose.</summary>
/// <param name="InternalOwnerInvocation">Captured internal-owner flag to restore on dispose.</param>
[Immutable]
[StructLayout(LayoutKind.Auto)]
internal readonly record struct RemoteInvocationScope(bool InternalOwnerInvocation) : IDisposable
{
    public void Dispose() => RemoteInvocationContext.RestoreInternalOwnerInvocation(InternalOwnerInvocation);
}
