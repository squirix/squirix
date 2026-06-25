using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;

namespace Squirix.Server.TestKit.Testing;

/// <summary>Allocation-friendly helpers for <see cref="List{T}" /> in tests and test kit code.</summary>
internal static class ListKit
{
    /// <summary>Invokes <paramref name="action" /> for each element without enumerator allocation.</summary>
    /// <typeparam name="T">Element type.</typeparam>
    /// <param name="list">List to walk.</param>
    /// <param name="action">Per-element callback.</param>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static void ForEach<T>(List<T> list, Action<T> action)
    {
        ArgumentNullException.ThrowIfNull(list);
        ArgumentNullException.ThrowIfNull(action);

        var items = CollectionsMarshal.AsSpan(list);
        for (var i = 0; i < items.Length; i++)
            action(items[i]);
    }
}
