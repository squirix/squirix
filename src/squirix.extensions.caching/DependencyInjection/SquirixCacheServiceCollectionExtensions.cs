using System;
using Microsoft.Extensions.Caching.Distributed;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Squirix.Extensions.Caching;

namespace Microsoft.Extensions.DependencyInjection;

/// <summary>Registers Squirix as the application's distributed cache provider.</summary>
public static class SquirixCacheServiceCollectionExtensions
{
    /// <summary>Replaces unkeyed distributed cache registrations with one Squirix singleton.</summary>
    /// <param name="services">The application service collection.</param>
    /// <param name="configure">Configures endpoints, authentication and the cache name.</param>
    /// <returns>The service collection for chaining.</returns>
    /// <remarks>Repeated calls compose configuration in registration order and preserve keyed registrations.</remarks>
    /// <exception cref="ArgumentNullException">An argument is null.</exception>
    public static IServiceCollection AddSquirixCache(this IServiceCollection services, Action<SquirixCacheOptions> configure)
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentNullException.ThrowIfNull(configure);
        _ = services.Configure(configure);
        _ = services.RemoveAll<IDistributedCache>();
        return services.AddSingleton<IDistributedCache, SquirixDistributedCache>();
    }
}
