using Squirix.Client;

namespace Squirix.Extensions.Caching;

/// <summary>Configures the Squirix provider for Microsoft distributed caching.</summary>
public sealed class SquirixCacheOptions
{
    /// <summary>Gets or sets the logical Squirix cache name used for all keys.</summary>
    public string CacheName { get; set; } = "distributed-cache";

    /// <summary>Gets the native client options for bootstrap endpoints and bearer authentication.</summary>
    /// <remarks>The provider sends bytes directly. Serializer settings and JSON contexts are unused.</remarks>
    public SquirixClientOptions ClientOptions { get; } = new();
}
