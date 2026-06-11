namespace Squirix.Server.Node.MemoryPressure;

/// <summary>
/// Supplies the process memory budget used to resolve and cap <see cref="MemoryPressureOptions.MaxEstimatedCacheBytes" />.
/// </summary>
internal interface IMemoryBudgetProvider
{
    /// <summary>
    /// Gets the total memory available to the process in bytes.
    /// </summary>
    /// <returns>Available process memory in bytes.</returns>
    long GetTotalAvailableBytes();
}
