using System;
using Microsoft.Extensions.Logging;

namespace Squirix.Server.Utils;

/// <summary>Replica group verification logs.</summary>
internal static partial class LogManager
{
    [LoggerMessage(EventId = 4001, Level = LogLevel.Warning, Message = "Replica group verification cannot proceed: the leader log tail is not fully committed or its log is not ready")]
    internal static partial void ReplicaVerificationBlocked(ILogger logger);

    [LoggerMessage(EventId = 4002, Level = LogLevel.Information, Message = "Replica group verification is complete: every slot counts toward the write quorum")]
    internal static partial void ReplicaVerificationComplete(ILogger logger);

    [LoggerMessage(EventId = 4004, Level = LogLevel.Information, Message = "Replica group verification is pending: some followers are not yet verified against the leader log")]
    internal static partial void ReplicaVerificationPending(ILogger logger);

    [LoggerMessage(EventId = 4003, Level = LogLevel.Debug, Message = "Replica group verification attempt failed and will be retried")]
    internal static partial void ReplicaVerificationRetry(ILogger logger, Exception exception);
}
