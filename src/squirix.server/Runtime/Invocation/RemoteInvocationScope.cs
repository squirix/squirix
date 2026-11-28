using System;
using System.Runtime.InteropServices;
using Squirix.Server.Attributes;

namespace Squirix.Server.Runtime.Invocation;

/// <summary>Restores <see cref="RemoteInvocationContext" /> async-local state on dispose.</summary>
/// <param name="PreviousInvocation">Captured internal-owner flag to restore on dispose.</param>
/// <param name="PreviousUnknownMutationOutcome">Whether the previous caller needed ambiguous writes reported explicitly.</param>
[Immutable]
[StructLayout(LayoutKind.Auto)]
internal readonly record struct RemoteInvocationScope(bool PreviousInvocation, bool PreviousUnknownMutationOutcome) : IDisposable
{
    public void Dispose() => RemoteInvocationContext.RestoreInvocation(PreviousInvocation, PreviousUnknownMutationOutcome);
}
