namespace Squirix.Server.Node.App.Operations;

/// <summary>
/// Stable, low-cardinality logical cache operation names shared across operational sinks (metrics, tracing, and similar).
/// </summary>
internal static class CacheOperationNames
{
    internal const string Add = "add";
    internal const string Contains = "contains";
    internal const string Get = "get";
    internal const string GetEntry = "get_entry";
    internal const string GetExpiration = "get_expiration";
    internal const string Set = "set";
    internal const string RemoveExpiration = "remove_expiration";
    internal const string Remove = "remove";
    internal const string Touch = "touch";
    internal const string TryAdd = "try_add";
    internal const string TryGet = "try_get";
    internal const string TryRemove = "try_remove";
}
