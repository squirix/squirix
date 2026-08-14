using Microsoft.Extensions.Logging;

namespace Squirix.Server;

/// <summary>Local in-memory cache diagnostics.</summary>
internal static partial class LogManager
{
    [LoggerMessage(
        EventId = 4001,
        Level = LogLevel.Debug,
        Message = "PhysicalCache UpdateAsync exhausted {MaxAttempts} optimistic-concurrency retries for key {Namespace}:{Key}.")]
    internal static partial void PhysicalCacheUpdateRetriesExhausted(ILogger logger, int maxAttempts, string @namespace, string key);
}
