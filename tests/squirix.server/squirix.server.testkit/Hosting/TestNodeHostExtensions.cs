using System;
using Microsoft.Extensions.DependencyInjection;
using Squirix.Server.Cluster;
using Squirix.Server.Runtime;
using Squirix.Server.Runtime.Contracts;

namespace Squirix.Server.TestKit.Hosting;

/// <summary>Test-only shortcuts over <see cref="ITestNodeHost" /> services.</summary>
internal static class TestNodeHostExtensions
{
    /// <param name="host">A started in-process test node.</param>
    extension(ITestNodeHost host)
    {
        /// <summary>Finds a cache key owned by the given node.</summary>
        /// <param name="cacheName">Target cache namespace.</param>
        /// <param name="owner">Expected owner node identifier.</param>
        /// <returns>A key owned by <paramref name="owner" />.</returns>
        /// <exception cref="InvalidOperationException">Thrown when no owned key was found.</exception>
        internal string FindKeyOwnedBy(string cacheName, string owner)
        {
            ArgumentNullException.ThrowIfNull(host);
            var locator = host.Services.GetRequiredService<INodeLocator>();
            for (var i = 0; i < 10_000; i++)
            {
                var candidate = $"{cacheName}-{i}";
                if (string.Equals(locator.GetOwner(cacheName, candidate), owner, StringComparison.Ordinal))
                    return candidate;
            }

            throw new InvalidOperationException($"No key owned by '{owner}' was found.");
        }

        /// <summary>Resolves a namespaced cache from the node's runtime.</summary>
        /// <typeparam name="T">Cached value type.</typeparam>
        /// <param name="cacheName">Target cache namespace.</param>
        /// <returns>The requested cache.</returns>
        internal ILogicalNamespacedCache<T> GetCache<T>(string cacheName)
        {
            ArgumentNullException.ThrowIfNull(host);
            return host.Services.GetRequiredService<ICacheRuntime>().GetCache<T>(cacheName);
        }

        /// <summary>Resolves a required service from the node's root service provider.</summary>
        /// <typeparam name="T">Service type.</typeparam>
        /// <returns>The resolved service.</returns>
        internal T GetRequiredService<T>()
            where T : notnull
        {
            ArgumentNullException.ThrowIfNull(host);
            return host.Services.GetRequiredService<T>();
        }

        /// <summary>Resolves a service from the node's root service provider, if registered.</summary>
        /// <typeparam name="T">Service type.</typeparam>
        /// <returns>The resolved service, or <see langword="null" /> when not registered.</returns>
        internal T? GetService<T>()
        {
            ArgumentNullException.ThrowIfNull(host);
            return host.Services.GetService(typeof(T)) is T service ? service : default;
        }
    }
}
