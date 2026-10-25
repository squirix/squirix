namespace Squirix.Server.Node.MemoryPressure;

/// <summary>Reads whether the node's estimated cache memory is in the critical pressure band.</summary>
internal interface ICriticalMemoryPressureReader
{
    /// <summary>Determines whether the node is under critical memory pressure right now.</summary>
    /// <returns><see langword="true" /> when the estimated cache memory is in the critical band; otherwise <see langword="false" />.</returns>
    bool IsCritical();
}
