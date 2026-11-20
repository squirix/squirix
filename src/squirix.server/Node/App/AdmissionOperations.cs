namespace Squirix.Server.Node.App;

/// <summary>Bounded operation names for memory admission metrics and diagnostics correlation (low cardinality).</summary>
internal static class AdmissionOperations
{
    internal const string Set = "set";

    internal const string TryAdd = "try_add";

    internal const string Unknown = "unknown";
}
