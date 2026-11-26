using System.Diagnostics;
using System.Threading;
using Squirix.Server.Attributes;

namespace Squirix.Server.LocalCache;

/// <summary>Tracks one occupant in its eviction scope’s ordering.</summary>
/// <typeparam name="T">The stored value type.</typeparam>
[ThreadSafe]
internal sealed class EvictionNode<T>
{
    private long _stamp;

    internal EvictionNode(EvictionScope<T> scope)
    {
        Scope = scope;
    }

    internal CacheSlot<T> Occupant { get; set; }

    internal EvictionScope<T> Scope { get; }

    internal EvictionNode<T>? Older { get; set; }

    internal EvictionNode<T>? Newer { get; set; }

    internal int Position
    {
        get => Volatile.Read(ref field);
        set => Volatile.Write(ref field, value);
    }

    internal long Stamp => Volatile.Read(ref _stamp);

    internal void MarkRecent() => Volatile.Write(ref _stamp, Stopwatch.GetTimestamp());
}
