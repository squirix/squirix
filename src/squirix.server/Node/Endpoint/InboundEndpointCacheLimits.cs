namespace Squirix.Server.Node.Endpoint;

/// <summary>Bounds the endpoint cache facade retention.</summary>
internal static class InboundEndpointCacheLimits
{
    internal const int MaxCachedFacades = 1024;
}
