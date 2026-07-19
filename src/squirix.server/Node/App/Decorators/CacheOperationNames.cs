namespace Squirix.Server.Node.App.Decorators;

/// <summary>Stable, low-cardinality logical cache operation names shared across operational sinks (metrics, tracing, and similar).</summary>
internal static class CacheOperationNames
{
    internal const string Get = "get";
    internal const string GetEntry = "get_entry";
    internal const string Remove = "remove";
    internal const string RemoveExpiration = "remove_expiration";
    internal const string Set = "set";
    internal const string Touch = "touch";
    internal const string TryAdd = "try_add";
    internal const string Update = "update";
}
