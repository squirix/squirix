using System;
using Microsoft.Extensions.Logging;

namespace Squirix.Server.Utils;

/// <summary>Internode gRPC client pool drain and disposal logs.</summary>
internal static partial class ServerLog
{
    [LoggerMessage(EventId = 5001, Level = LogLevel.Debug, Message = "Failed to dispose server call policy for node {NodeId} during pool drain")]
    internal static partial void ClientPoolPolicyDisposeFailed(ILogger logger, Exception exception, string nodeId);

    [LoggerMessage(EventId = 5002, Level = LogLevel.Debug, Message = "Failed to dispose gRPC channel for node {NodeId} during pool drain")]
    internal static partial void ClientPoolChannelDisposeFailed(ILogger logger, Exception exception, string nodeId);

    [LoggerMessage(
        EventId = 5003,
        Level = LogLevel.Warning,
        Message = "Server client pool did not drain within the shutdown budget of {Budget}; peers still busy: {BusyPeers}. Their channels are disposed anyway")]
    internal static partial void ClientPoolDrainTimedOut(ILogger logger, TimeSpan budget, string busyPeers);
}
