using System;
using System.Collections;
using System.Collections.Generic;

namespace Squirix.Server.UnitTests.Support;

/// <summary>Zero-array <see cref="IReadOnlyList{T}" /> wrapper for a single test value.</summary>
/// <typeparam name="T">Element type.</typeparam>
internal sealed class SingleItemReadOnlyList<T> : IReadOnlyList<T>
{
    private readonly T _item;

    internal SingleItemReadOnlyList(T item) => _item = item;

    public int Count => 1;

    public T this[int index] => index is 0 ? _item : throw new ArgumentOutOfRangeException(nameof(index));

    public IEnumerator<T> GetEnumerator()
    {
        yield return _item;
    }

    IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();
}
