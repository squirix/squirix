namespace Squirix.Server.Cluster.Transport;

/// <summary>Connection feature that only the internal mTLS listener sets.</summary>
internal sealed class InternalListenerMarker
{
    internal static readonly InternalListenerMarker Instance = new();

    private InternalListenerMarker()
    {
    }
}
