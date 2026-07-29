using System;
using System.Collections.Frozen;
using System.Collections.Generic;

namespace Squirix.Server.TestKit;

/// <summary>Allocation-light builders for entry-tag fixtures used by server tests.</summary>
public static class EntryTagsKit
{
    /// <summary>Gets the cached <c>region=west</c> tag set shared by codec/store fixtures.</summary>
    public static FrozenDictionary<string, string> RegionWest { get; } = One("region", "west");

    /// <summary>Builds a one-entry tag map without a temporary <see cref="Dictionary{TKey,TValue}" />.</summary>
    /// <param name="key">Tag key.</param>
    /// <param name="value">Tag value.</param>
    /// <returns>A frozen dictionary containing only <paramref name="key" />.</returns>
    public static FrozenDictionary<string, string> One(string key, string value)
    {
        KeyValuePair<string, string>[] pairs = [new(key, value)];
        return pairs.ToFrozenDictionary(StringComparer.Ordinal);
    }

    /// <summary>Builds <paramref name="count" /> tags with invariant index keys and value <c>v</c>.</summary>
    /// <param name="count">Number of tags to create.</param>
    /// <returns>A frozen dictionary with <paramref name="count" /> entries.</returns>
    public static FrozenDictionary<string, string> CreateCount(int count)
    {
        ArgumentOutOfRangeException.ThrowIfNegative(count);
        if (count is 0)
            return FrozenDictionary<string, string>.Empty;

        var pairs = new KeyValuePair<string, string>[count];
        for (var i = 0; i < count; i++)
            pairs[i] = new KeyValuePair<string, string>(NodeInvariantIndexStrings.Format(i), "v");

        return pairs.ToFrozenDictionary(StringComparer.Ordinal);
    }
}
