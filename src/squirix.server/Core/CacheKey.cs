using System;
using Squirix.Server.Attributes;

namespace Squirix.Server.Core;

[Immutable]
internal sealed record CacheKey(string Namespace, string Key)
{
    public override string ToString()
    {
        if (string.IsNullOrEmpty(Namespace))
            return Key;

        return string.Create(
            Namespace.Length + 1 + Key.Length,
            (Namespace, Key),
            static (span, state) =>
            {
                state.Namespace.AsSpan().CopyTo(span);
                span[state.Namespace.Length] = ':';
                state.Key.AsSpan().CopyTo(span[(state.Namespace.Length + 1)..]);
            });
    }

    internal static CacheKey Default(string key) => new(ServerCacheNames.DefaultNamespace, key);
}
