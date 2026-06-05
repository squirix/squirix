namespace Squirix.Server.Node.App.Operations;

/// <summary>
/// Stable, low-cardinality logical cache operation outcome categories shared across operational sinks (metrics, tracing, and similar).
/// </summary>
internal static class CacheOperationResults
{
    internal const string Cancelled = "cancelled";
    internal const string DeadlineExceeded = "deadline_exceeded";
    internal const string Failed = "failed";
    internal const string InvalidArgument = "invalid_argument";
    internal const string NotFound = "not_found";
    internal const string Ok = "ok";
    internal const string ResourceExhausted = "resource_exhausted";
}
