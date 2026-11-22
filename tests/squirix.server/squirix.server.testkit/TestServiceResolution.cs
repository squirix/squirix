using System;
using System.Collections.Generic;
using Squirix.Server.Composition;

namespace Squirix.Server.TestKit;

/// <summary>Resolves test fixtures and registered node components.</summary>
internal static class TestServiceResolution
{
    internal static T GetRequiredService<T>(this IServiceProvider provider)
    {
        return provider.GetService(typeof(T)) is T value
            ? value
            : throw new InvalidOperationException($"No service for type '{typeof(T)}' has been registered.");
    }

    internal static T? GetService<T>(this IServiceProvider provider) => provider.GetService(typeof(T)) is T value ? value : default;

    internal static IEnumerable<T> GetServices<T>(this IServiceProvider provider) =>
        provider is NodeProvider node ? node.All<T>() : provider.GetService<IEnumerable<T>>() ?? [];
}
