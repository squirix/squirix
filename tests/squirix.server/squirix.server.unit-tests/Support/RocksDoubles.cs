using Squirix.Server.Node.MemoryPressure;

namespace Squirix.Server.UnitTests.Support;

/// <summary>Rocks-backed factories for trivial shared cache test doubles (no delegate allocations).</summary>
internal static class RocksDoubles
{
    /// <summary>Creates an <see cref="IMemoryBudgetProvider" /> mock with a fixed available-byte value.</summary>
    /// <param name="availableBytes">Fixed the available memory budget in bytes.</param>
    internal static IMemoryBudgetProvider CreateMemoryBudget(long availableBytes)
    {
        var expectations = new IMemoryBudgetProviderCreateExpectations();
        _ = expectations.Setups.GetTotalAvailableBytes().ReturnValue(availableBytes);
        return expectations.Instance();
    }
}
