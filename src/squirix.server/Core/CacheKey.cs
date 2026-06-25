using System;

namespace Squirix.Server.Core;

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

    public static implicit operator string(CacheKey key)
    {
        return key.Key;
    }

    public static bool operator <(CacheKey left, CacheKey right)
    {
        return left.CompareTo(right) < 0;
    }

    public static bool operator <=(CacheKey left, CacheKey right)
    {
        return left.CompareTo(right) <= 0;
    }

    public static bool operator >(CacheKey left, CacheKey right)
    {
        return left.CompareTo(right) > 0;
    }

    public static bool operator >=(CacheKey left, CacheKey right)
    {
        return left.CompareTo(right) >= 0;
    }

    public static CacheKey Default(string key) => new(CacheNames.DefaultNamespace, key);

    public override string ToString() => string.IsNullOrEmpty(Namespace) ? Key : Namespace + ":" + Key;

    public int CompareTo(CacheKey other)
    {
        var namespaceComparison = string.CompareOrdinal(Namespace, other.Namespace);
        return namespaceComparison is not 0 ? namespaceComparison : string.CompareOrdinal(Key, other.Key);
    }
}
