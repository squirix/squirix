namespace Squirix.Server.Node.MemoryPressure;

/// <summary>Bounded operation names for memory admission metrics and diagnostics correlation (low cardinality).</summary>
internal static class AdmissionOperations
{
    public const string Set = "set";

    public const string TryAdd = "try_add";

    public const string Unknown = "unknown";
}
