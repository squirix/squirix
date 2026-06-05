using System;

namespace Squirix.Server.Runtime.Contracts;

/// <summary>
/// Enters an async-local remote-invocation scope for server endpoint handlers and interceptors.
/// </summary>
internal interface IRemoteInvocationScopeFactory
{
    /// <summary>
    /// Enters a remote-invocation scope and returns a disposable that restores the previous scope.
    /// </summary>
    /// <param name="isInternalOwnerInvocation">Whether the invocation is an internal owner-routed cluster RPC.</param>
    /// <returns>A disposable scope handle.</returns>
    IDisposable EnterRemoteInvocation(bool isInternalOwnerInvocation);
}
