using System;
using System.Collections.Generic;

namespace Squirix.Server.Composition;

internal static class NodeResolution
{
    extension(IServiceProvider provider)
    {
#pragma warning disable RS0030 // This is the node composition boundary for explicitly registered factories.
        internal T Require<T>() => provider.GetService(typeof(T)) is T value ? value : throw new InvalidOperationException($"Missing node component {typeof(T)}.");

        internal T? Find<T>()
            where T : class => provider.GetService(typeof(T)) as T;
#pragma warning restore RS0030

        internal IEnumerable<T> All<T>() => provider is NodeProvider node ? node.All<T>() : provider.Require<IEnumerable<T>>();
    }
}
