namespace Squirix.Server.Runtime.Invocation;

/// <summary>Headers shared by the sending and receiving sides of peer forwarding.</summary>
internal static class RemoteInvocationContract
{
    /// <summary>HTTP request header that carries the ring fingerprint of the calling node on internal owner-routed RPCs.</summary>
    internal const string RingFingerprintHeaderName = "squirix-ring-fingerprint";
}
