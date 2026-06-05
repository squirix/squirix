namespace Squirix.Server.Runtime.Contracts;

/// <summary>
/// Reads the current async-local remote-invocation classification for endpoint routing decisions.
/// </summary>
internal interface IRemoteInvocationState
{
    /// <summary>
    /// Gets a value indicating whether the current execution is an internal owner-routed RPC from another cluster node.
    /// </summary>
    bool IsInternalOwnerInvocation { get; }
}
