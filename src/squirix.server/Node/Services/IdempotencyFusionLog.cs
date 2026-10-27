using System;
using Microsoft.Extensions.Logging;

namespace Squirix.Server.Node.Services;

/// <summary>Logs of the idempotency outcome frame that is appended together with its mutation frame.</summary>
internal static partial class IdempotencyFusionLog
{
    [LoggerMessage(
        EventId = 1021,
        Level = LogLevel.Debug,
        Message = "The journal refused the outcome frame before it reached the ring; the outcome is recorded after the memory apply instead")]
    internal static partial void AppendRefused(ILogger logger, Exception exception);

    [LoggerMessage(
        EventId = 1020,
        Level = LogLevel.Warning,
        Message = "The outcome of the write could not be projected to its response; the outcome is recorded after the memory apply instead")]
    internal static partial void ProjectionFailed(ILogger logger, Exception exception);
}
