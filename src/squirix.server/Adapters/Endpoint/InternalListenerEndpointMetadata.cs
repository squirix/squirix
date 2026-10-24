namespace Squirix.Server.Adapters.Endpoint;

/// <summary>
/// Pairs the anonymous internal-listener mapping with its externally authorized twin: the internal endpoint is served only to
/// certificate-verified peers on the internal listener port, the external one only to everyone else.
/// </summary>
internal sealed class InternalListenerEndpointMetadata
{
    internal InternalListenerEndpointMetadata(int internalListenPort, bool internalOnly)
    {
        InternalListenPort = internalListenPort;
        InternalOnly = internalOnly;
    }

    internal int InternalListenPort { get; }

    internal bool InternalOnly { get; }
}
