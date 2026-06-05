namespace Squirix.Server.Runtime.Contracts;

/// <summary>
/// Stable remote-invocation transport markers shared by gRPC adapters and cluster client calls.
/// </summary>
internal static class RemoteInvocationContract
{
    /// <summary>
    /// gRPC request metadata key for internal owner-routed RPC classification.
    /// </summary>
    public const string InternalOwnerRpcHeaderName = "squirix-internal-owner-rpc";

    /// <summary>
    /// gRPC request metadata value for internal owner-routed RPC classification.
    /// </summary>
    public const string InternalOwnerRpcHeaderValue = "true";
}
