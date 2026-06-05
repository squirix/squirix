using System;
using Squirix.Server.Runtime.Contracts;

namespace Squirix.Server.Node.Context;

/// <summary>
/// DI-backed accessor for <see cref="RemoteInvocationContext" /> async-local state.
/// </summary>
internal sealed class RemoteInvocationContextService : IRemoteInvocationScopeFactory, IRemoteInvocationState
{
    /// <inheritdoc />
    public bool IsInternalOwnerInvocation => RemoteInvocationContext.IsInternalOwnerInvocation;

    /// <inheritdoc />
    public IDisposable EnterRemoteInvocation(bool isInternalOwnerInvocation) => RemoteInvocationContext.EnterRemoteInvocation(isInternalOwnerInvocation);
}
