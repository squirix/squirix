using System;
using Squirix.Server.Attributes;

namespace Squirix.Server.Core;

/// <summary>The namespace and key an entry is stored under.</summary>
/// <param name="Namespace">The cache namespace.</param>
/// <param name="Key">The key within the namespace.</param>
/// <remarks>A value type, so a lookup builds no object; equality compares both strings ordinally.</remarks>
[Immutable]
internal readonly record struct CacheKey(string Namespace, string Key)
{
    public override string ToString() => string.IsNullOrEmpty(Namespace) ? Key : Compose(Namespace, Key);

    public override int GetHashCode() => Key == null ? NamespaceHash(Namespace) : LookupHash(Namespace, Key);

    internal static int LookupHash(string cacheName, ReadOnlySpan<char> key) => unchecked(NamespaceHash(cacheName) + string.GetHashCode(key, StringComparison.Ordinal));

    internal static CacheKey Default(string key) => new(ServerCacheNames.DefaultNamespace, key);

    private static int NamespaceHash(string? cacheName) => cacheName == null ? 0 : unchecked(StringComparer.Ordinal.GetHashCode(cacheName) * -1521134295);

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
