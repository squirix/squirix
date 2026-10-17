using System;
using Squirix.Server.Attributes;

namespace Squirix.Server.Core;

[Immutable]
internal sealed record CacheKey(string Namespace, string Key)
{
    public override string ToString() => string.IsNullOrEmpty(Namespace) ? Key : Compose(Namespace, Key);

    internal static CacheKey Default(string key) => new(ServerCacheNames.DefaultNamespace, key);

    private static string Compose(string namespaceName, string key) => string.Create(
        namespaceName.Length + 1 + key.Length,
        (Namespace: namespaceName, Key: key),
        static (span, state) =>
        {
            state.Namespace.AsSpan().CopyTo(span);
            span[state.Namespace.Length] = ':';
            state.Key.AsSpan().CopyTo(span[(state.Namespace.Length + 1)..]);
        });
}
