namespace Squirix.Server.Runtime.Invocation;

/// <summary>Stable remote-invocation transport markers shared by gRPC adapters and cluster client calls.</summary>
internal static class RemoteInvocationContract
{
    /// <summary>gRPC request metadata key for internal owner-routed RPC classification.</summary>
    public const string InternalOwnerRpcHeaderName = "squirix-internal-owner-rpc";

    /// <summary>gRPC request metadata value for internal owner-routed RPC classification.</summary>
    public const string InternalOwnerRpcHeaderValue = "true";

    /// <summary>gRPC request metadata key that carries the ring fingerprint of the calling node on internal owner-routed RPCs.</summary>
    public const string RingFingerprintHeaderName = "squirix-ring-fingerprint";
}
