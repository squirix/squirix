using System;

namespace Squirix.Server.Node.MemoryPressure;

/// <summary>
/// Uses <see cref="GC.GetGCMemoryInfo()" /> to read the process memory budget.
/// </summary>
internal sealed class GcMemoryBudgetProvider : IMemoryBudgetProvider
{
    /// <summary>
    /// Gets the shared provider for production bootstrap and settings validation.
    /// </summary>
    internal static GcMemoryBudgetProvider Instance { get; } = new();

    /// <inheritdoc />
    public long GetTotalAvailableBytes() => GC.GetGCMemoryInfo().TotalAvailableMemoryBytes;
}
