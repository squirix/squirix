using System;
using System.Collections;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using Squirix.Server.Attributes;

namespace Squirix.Server.Storage.Replication;

/// <summary>Read-only, ascending-order view over a <see cref="SortedDictionary{TKey, TValue}" />.</summary>
/// <remarks>
/// The view wraps the source dictionary by reference and does not copy it. Mutations to the source are visible
/// through this instance, and a mutation during enumeration invalidates the enumerator. Do not retain the view
/// or an enumerator across a mutation of the backing dictionary.
/// </remarks>
/// <typeparam name="TKey">The key type.</typeparam>
/// <typeparam name="TValue">The value type.</typeparam>
[Immutable]
internal sealed class ReadOnlySortedDictionary<TKey, TValue> : IReadOnlyDictionary<TKey, TValue>
    where TKey : notnull
{
    private readonly SortedDictionary<TKey, TValue> _dictionary;

    internal ReadOnlySortedDictionary(SortedDictionary<TKey, TValue> dictionary)
    {
        ArgumentNullException.ThrowIfNull(dictionary);
        _dictionary = dictionary;
    }

    /// <inheritdoc />
    public int Count => _dictionary.Count;

    /// <inheritdoc />
    public IEnumerable<TKey> Keys => _dictionary.Keys;

    /// <inheritdoc />
    public IEnumerable<TValue> Values => _dictionary.Values;

    /// <inheritdoc />
    public TValue this[TKey key] => _dictionary[key];

    /// <inheritdoc />
    public bool ContainsKey(TKey key) => _dictionary.ContainsKey(key);

    /// <inheritdoc />
    public bool TryGetValue(TKey key, [MaybeNullWhen(false)] out TValue value) => _dictionary.TryGetValue(key, out value);

    /// <inheritdoc />
    public IEnumerator<KeyValuePair<TKey, TValue>> GetEnumerator() => _dictionary.GetEnumerator();

    IEnumerator IEnumerable.GetEnumerator() => _dictionary.GetEnumerator();
}
