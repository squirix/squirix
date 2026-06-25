using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;

namespace Squirix.Server.Utils;

internal static class ListEx
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
