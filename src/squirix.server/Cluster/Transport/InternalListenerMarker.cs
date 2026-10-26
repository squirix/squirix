using Squirix.Server.Attributes;

namespace Squirix.Server.Cluster.Transport;

/// <summary>Connection feature that only the internal mTLS listener sets.</summary>
[Immutable]
internal sealed class InternalListenerMarker
{
    internal static readonly InternalListenerMarker Instance = new();

    private InternalListenerMarker()
    {
    }
}
