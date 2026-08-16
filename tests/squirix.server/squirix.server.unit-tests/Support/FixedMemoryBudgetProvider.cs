using Squirix.Attributes;
using Squirix.Server.Node.MemoryPressure;

namespace Squirix.Server.UnitTests.Support;

/// <summary>Test memory-budget provider with a fixed available-byte value.</summary>
[Immutable]
internal sealed class FixedMemoryBudgetProvider : IMemoryBudgetProvider
{
    private readonly long _availableBytes;

    /// <summary>Initializes a new instance of the <see cref="FixedMemoryBudgetProvider" /> class.</summary>
    /// <param name="availableBytes">Fixed available memory budget in bytes.</param>
    internal FixedMemoryBudgetProvider(long availableBytes)
    {
        _availableBytes = availableBytes;
    }

    /// <inheritdoc />
    long IMemoryBudgetProvider.GetTotalAvailableBytes() => _availableBytes;
}
