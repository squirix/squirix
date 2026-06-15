using System;

namespace Squirix.Server.Core;

internal readonly record struct CacheKey(string Namespace, string Key) : IComparable<CacheKey>
{
    public static implicit operator CacheKey(string key)
    {
        return Default(key);
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
        var namespaceComparison = string.Compare(Namespace, other.Namespace, StringComparison.Ordinal);
        return namespaceComparison != 0 ? namespaceComparison : string.Compare(Key, other.Key, StringComparison.Ordinal);
    }
}
