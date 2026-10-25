using System.Threading;
using Squirix.Server.Attributes;
using Squirix.Server.Node.MemoryPressure;

namespace Squirix.Server.UnitTests.Support;

/// <summary>A critical-pressure reader whose answer a test flips between calls.</summary>
[ThreadSafe]
internal sealed class SwitchableMemoryPressure : ICriticalMemoryPressureReader
{
    private int _critical;

    /// <inheritdoc />
    public bool IsCritical() => Volatile.Read(ref _critical) != 0;

    /// <summary>Sets whether the node reports critical pressure.</summary>
    /// <param name="critical"><see langword="true" /> to report critical pressure.</param>
    internal void Set(bool critical) => Volatile.Write(ref _critical, critical ? 1 : 0);
}
